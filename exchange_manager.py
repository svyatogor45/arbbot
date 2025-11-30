# exchange_manager.py
# ---------------------------------------------------
# Менеджер бирж. Слой-обёртка над CCXT (async).
#
# Улучшения:
#   - Кэширование market info
#   - Проверка min_order_size
#   - Health check соединений
#   - Rate limit tracking
# ---------------------------------------------------

import asyncio
import time
from typing import Dict, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field

import ccxt.async_support as ccxt
import ccxt as ccxt_sync
from loguru import logger

from config import EXCHANGES, DEFAULT_MARKET_TYPE
from symbol_mapper import to_ccxt_symbol, pretty


# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

# Время жизни кэша market info (секунды)
# Оптимизировано: 300s → 3600s (лимиты бирж меняются очень редко)
MARKET_INFO_CACHE_TTL = 3600  # 1 час

# Интервал health check (секунды)
HEALTH_CHECK_INTERVAL = 60

# Порог для предупреждения о rate limit
RATE_LIMIT_WARNING_THRESHOLD = 0.8  # 80% от лимита


# ============================================================
# МЕТРИКИ И КЭШИРОВАНИЕ
# ============================================================

@dataclass
class CachedMarketInfo:
    """Закэшированная информация о рынке."""
    data: Dict[str, Any]
    timestamp: float
    
    def is_valid(self, ttl: float = MARKET_INFO_CACHE_TTL) -> bool:
        return (time.time() - self.timestamp) < ttl


@dataclass
class ExchangeHealth:
    """Метрики здоровья соединения с биржей."""
    exchange: str
    connected: bool = False
    last_request_ts: float = 0.0
    last_success_ts: float = 0.0
    last_error_ts: float = 0.0
    last_error_msg: str = ""
    requests_total: int = 0
    requests_success: int = 0
    requests_failed: int = 0
    rate_limit_hits: int = 0
    markets_loaded: int = 0
    
    @property
    def success_rate(self) -> float:
        if self.requests_total == 0:
            return 100.0
        return (self.requests_success / self.requests_total) * 100
    
    @property
    def is_healthy(self) -> bool:
        """Соединение считается здоровым если успешность > 90%."""
        if not self.connected:
            return False
        if self.requests_total < 5:
            return True  # Мало данных для оценки
        return self.success_rate > 90
    
    def record_request(self, success: bool, error_msg: str = ""):
        """Записать результат запроса."""
        self.requests_total += 1
        self.last_request_ts = time.time()
        
        if success:
            self.requests_success += 1
            self.last_success_ts = time.time()
        else:
            self.requests_failed += 1
            self.last_error_ts = time.time()
            self.last_error_msg = error_msg
    
    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "connected": self.connected,
            "is_healthy": self.is_healthy,
            "success_rate": f"{self.success_rate:.1f}%",
            "requests_total": self.requests_total,
            "requests_failed": self.requests_failed,
            "rate_limit_hits": self.rate_limit_hits,
            "markets_loaded": self.markets_loaded,
            "last_error": self.last_error_msg[:50] if self.last_error_msg else None,
        }


class ExchangeManager:
    """
    ExchangeManager
    ----------------
    Единая точка входа для работы с биржами через CCXT.

    Возможности:
      - Кэширование инстансов бирж
      - Кэширование market info
      - Проверка минимальных размеров ордеров
      - Мониторинг здоровья соединений
      - Нормализация статусов ордеров
      - Классификация ошибок CCXT
    """

    def __init__(self, credentials_provider: Optional[Callable[[str], Dict[str, Any]]] = None):
        # Кеш активных инстансов бирж: { "bybit": <ccxt.bybit>, ... }
        self.active_exchanges: Dict[str, Any] = {}
        
        # Функция для получения credentials
        self.credentials_provider = credentials_provider
        
        # Кэш market info: { "bybit:BTC/USDT:USDT": CachedMarketInfo, ... }
        self._market_info_cache: Dict[str, CachedMarketInfo] = {}
        
        # Метрики здоровья по биржам
        self._health: Dict[str, ExchangeHealth] = {}
        
        # Lock для thread-safe создания инстансов
        self._create_lock = asyncio.Lock()

    # ============================================================
    # ВНУТРЕННИЕ ХЕЛПЕРЫ
    # ============================================================

    @staticmethod
    def _normalize_name(exchange_name: str) -> str:
        """Привести имя биржи к нижнему регистру."""
        return (exchange_name or "").strip().lower()

    @staticmethod
    def _normalize_order_status(
        raw_status: Optional[str],
        filled: Optional[float] = None,
        amount: Optional[float] = None,
    ) -> str:
        """
        Нормализовать статус ордера CCXT в один из:
          - 'filled'
          - 'open'
          - 'error'
        """
        if not raw_status:
            if filled and amount and filled > 0:
                return "open"
            return "error"

        s = raw_status.lower()

        if s in ("closed", "filled"):
            return "filled"

        if s in ("open", "partial", "partially_filled", "pending", "new"):
            try:
                if filled is not None and amount is not None and amount > 0:
                    fill_ratio = filled / amount
                    if fill_ratio >= 0.999:
                        return "filled"
            except Exception:
                pass
            return "open"

        if s in ("canceled", "cancelled", "rejected", "expired"):
            return "error"

        return "error"

    @staticmethod
    def _classify_exception(e: Exception) -> str:
        """
        Классифицировать исключение CCXT для верхнего уровня.
        
        Возвращает короткий код ошибки.
        """
        name = e.__class__.__name__

        # Rate limiting
        if isinstance(e, ccxt_sync.RateLimitExceeded):
            return "rate_limit"
        if isinstance(e, ccxt_sync.DDoSProtection):
            return "ddos_protection"

        # Сетевые ошибки
        if isinstance(e, ccxt_sync.NetworkError):
            if isinstance(e, ccxt_sync.ExchangeNotAvailable):
                return "exchange_not_available"
            if isinstance(e, ccxt_sync.RequestTimeout):
                return "timeout"
            return "network_error"

        # Ошибки торговой логики
        if isinstance(e, ccxt_sync.InsufficientFunds):
            return "insufficient_funds"
        if isinstance(e, ccxt_sync.InvalidOrder):
            # Дополнительная классификация InvalidOrder
            msg = str(e).lower()
            if "min" in msg or "minimum" in msg:
                return "below_min_size"
            if "precision" in msg:
                return "precision_error"
            return "invalid_order"
        if isinstance(e, ccxt_sync.AuthenticationError):
            return "auth_error"
        if isinstance(e, ccxt_sync.OrderNotFound):
            return "order_not_found"
        if isinstance(e, ccxt_sync.PermissionDenied):
            return "permission_denied"
        if isinstance(e, ccxt_sync.BadSymbol):
            return "bad_symbol"

        # Exchange-specific
        if isinstance(e, ccxt_sync.ExchangeError):
            return f"exchange_error:{name}"

        return f"ccxt_error:{name}"

    def _get_health(self, exchange_name: str) -> ExchangeHealth:
        """Получить или создать объект здоровья для биржи."""
        name = self._normalize_name(exchange_name)
        if name not in self._health:
            self._health[name] = ExchangeHealth(exchange=name)
        return self._health[name]

    def _build_exchange_config(self, name: str) -> Dict[str, Any]:
        """Собрать конфиг для CCXT-инстанса биржи."""
        config: Dict[str, Any] = {
            "enableRateLimit": True,
            "adjustForTimeDifference": True,
            "options": {
                "defaultType": DEFAULT_MARKET_TYPE,
            },
        }

        # Подмешиваем креды
        if self.credentials_provider:
            try:
                creds = self.credentials_provider(name) or {}
                if creds:
                    config.update(creds)
            except Exception as e:
                logger.error(f"⚠️ Не удалось получить креды для {name}: {e}")

        return config

    # ============================================================
    # СОЗДАНИЕ И УПРАВЛЕНИЕ ИНСТАНСАМИ
    # ============================================================

    async def _create_exchange_instance(self, exchange_name: str):
        """
        Создать новый инстанс биржи и прогрузить рынки.
        Thread-safe через asyncio.Lock.
        """
        name = self._normalize_name(exchange_name)
        health = self._get_health(name)

        if name not in EXCHANGES:
            logger.error(f"❌ Биржа '{name}' отсутствует в EXCHANGES.")
            return None

        if not hasattr(ccxt, name):
            logger.error(f"❌ CCXT не содержит реализацию биржи '{name}'.")
            return None

        exchange_class = getattr(ccxt, name)
        config = self._build_exchange_config(name)

        exchange = exchange_class(config)

        try:
            await exchange.load_markets()
            health.connected = True
            health.markets_loaded = len(exchange.markets)
            logger.info(
                f"✅ Биржа {name} подключена. Доступно рынков: {health.markets_loaded}"
            )
        except Exception as e:
            health.connected = False
            health.record_request(False, str(e))
            logger.error(f"❌ load_markets({name}) ошибка: {e}")
            try:
                await exchange.close()
            except Exception:
                pass
            return None

        return exchange

    async def load_exchange(self, exchange_name: str):
        """
        Вернуть инстанс биржи из кеша или создать новый.
        Thread-safe.
        """
        name = self._normalize_name(exchange_name)

        # Быстрая проверка без lock
        inst = self.active_exchanges.get(name)
        if inst:
            return inst

        # Создание с lock
        async with self._create_lock:
            # Повторная проверка после получения lock
            inst = self.active_exchanges.get(name)
            if inst:
                return inst

            inst = await self._create_exchange_instance(name)
            if not inst:
                return None

            self.active_exchanges[name] = inst
            return inst

    async def ensure_connected(self, exchange_name: str) -> bool:
        """Убедиться, что биржа подключена."""
        inst = await self.load_exchange(exchange_name)
        return inst is not None

    # ============================================================
    # MARKET INFO И MIN ORDER SIZE
    # ============================================================

    async def get_market_info(
        self, 
        exchange_name: str, 
        symbol: str,
        use_cache: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Получить информацию о рынке с кэшированием.
        """
        exchange = await self.load_exchange(exchange_name)
        if not exchange:
            return None

        name = self._normalize_name(exchange_name)
        ccxt_symbol = to_ccxt_symbol(exchange_name, symbol)
        cache_key = f"{name}:{ccxt_symbol}"

        # Проверяем кэш
        if use_cache:
            cached = self._market_info_cache.get(cache_key)
            if cached and cached.is_valid():
                return cached.data

        try:
            market = exchange.market(ccxt_symbol)
            
            # Сохраняем в кэш
            self._market_info_cache[cache_key] = CachedMarketInfo(
                data=market,
                timestamp=time.time()
            )
            
            return market
        except Exception as e:
            logger.error(f"❌ market_info({exchange_name}, {ccxt_symbol}): {e}")
            return None

    async def get_min_order_size(
        self,
        exchange_name: str,
        symbol: str,
        price: Optional[float] = None,
    ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        """
        Получить минимальные требования к размеру ордера.
        
        Returns:
            (min_amount, min_notional, precision_mode)
            
            min_amount: минимальное количество в базовой валюте
            min_notional: минимальная стоимость в quote валюте
            precision_mode: режим округления ('decimal', 'tick', etc.)
        """
        market = await self.get_market_info(exchange_name, symbol)
        if not market:
            return None, None, None

        limits = market.get("limits", {})
        amount_limits = limits.get("amount", {})
        cost_limits = limits.get("cost", {})
        
        min_amount = amount_limits.get("min")
        min_notional = cost_limits.get("min")
        
        # Precision info
        precision = market.get("precision", {})
        precision_mode = market.get("precisionMode", "decimal")
        
        # Конвертируем в float если не None
        if min_amount is not None:
            try:
                min_amount = float(min_amount)
            except (TypeError, ValueError):
                min_amount = None
                
        if min_notional is not None:
            try:
                min_notional = float(min_notional)
            except (TypeError, ValueError):
                min_notional = None

        return min_amount, min_notional, precision_mode

    async def validate_order_size(
        self,
        exchange_name: str,
        symbol: str,
        amount: float,
        price: float,
    ) -> Tuple[bool, str]:
        """
        Проверить, соответствует ли размер ордера требованиям биржи.
        
        Returns:
            (is_valid, reason)
        """
        if amount <= 0:
            return False, "non_positive_amount"
            
        if price <= 0:
            return False, "non_positive_price"

        min_amount, min_notional, _ = await self.get_min_order_size(
            exchange_name, symbol, price
        )

        if min_amount is not None and amount < min_amount:
            return False, f"below_min_amount:{min_amount}"

        if min_notional is not None:
            notional = amount * price
            if notional < min_notional:
                return False, f"below_min_notional:{min_notional}"

        return True, "ok"

    async def adjust_amount_precision(
        self,
        exchange_name: str,
        symbol: str,
        amount: float,
    ) -> Optional[float]:
        """
        Округлить amount до precision биржи.
        """
        exchange = await self.load_exchange(exchange_name)
        if not exchange:
            return None

        ccxt_symbol = to_ccxt_symbol(exchange_name, symbol)
        
        try:
            return exchange.amount_to_precision(ccxt_symbol, amount)
        except Exception as e:
            logger.warning(
                f"⚠️ amount_to_precision({exchange_name}, {symbol}): {e}"
            )
            return amount

    # ============================================================
    # ПОЛУЧЕНИЕ ЦЕНЫ
    # ============================================================

    async def get_price(self, exchange_name: str, symbol: str) -> Optional[float]:
        """Получить последнюю цену (ticker.last) по символу."""
        exchange = await self.load_exchange(exchange_name)
        if not exchange:
            return None

        name = self._normalize_name(exchange_name)
        health = self._get_health(name)
        ccxt_symbol = to_ccxt_symbol(exchange_name, symbol)

        try:
            ticker = await exchange.fetch_ticker(ccxt_symbol)
            health.record_request(True)
            return ticker.get("last")
        except Exception as e:
            code = self._classify_exception(e)
            health.record_request(False, code)
            
            if code == "rate_limit":
                health.rate_limit_hits += 1
                
            logger.error(
                f"❌ Цена ошибка [{exchange_name}] {ccxt_symbol}: {e} | code={code}"
            )
            return None

    # ============================================================
    # ОТПРАВКА ОРДЕРА
    # ============================================================

    async def place_order(
        self,
        exchange_name: str,
        symbol: str,
        side: str,
        amount: float,
        params: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """
        Выставить рыночный ордер.

        Возвращает словарь формата:
          {
              "status": "filled" | "open" | "error",
              "data": <raw order dict или None>,
              "msg": <строка с текстом ошибки или None>,
              "filled": <float или None>,
              "requested_amount": <float>,
              "average_price": <float или None>
          }
        """
        name = self._normalize_name(exchange_name)
        health = self._get_health(name)
        ccxt_symbol = to_ccxt_symbol(exchange_name, symbol)
        params = params or {}
        p_symbol = pretty(symbol)

        if amount <= 0:
            logger.error(
                f"❌ ORDER ERR [{exchange_name}] {side.upper()} "
                f"non-positive amount={amount} {p_symbol}"
            )
            return {
                "status": "error",
                "data": None,
                "msg": "non_positive_amount",
                "filled": None,
                "requested_amount": amount,
                "average_price": None,
            }

        exchange = await self.load_exchange(exchange_name)
        if not exchange:
            return {
                "status": "error",
                "data": None,
                "msg": "connection_failed",
                "filled": None,
                "requested_amount": amount,
                "average_price": None,
            }

        try:
            order = await exchange.create_order(
                symbol=ccxt_symbol,
                type="market",
                side=side,
                amount=amount,
                params=params,
            )

            health.record_request(True)

            raw_status = (order.get("status") or "").lower()
            filled = None
            requested_amount = amount
            average_price = None

            try:
                filled = float(order.get("filled")) if order.get("filled") is not None else None
            except (TypeError, ValueError):
                filled = None

            try:
                if order.get("amount") is not None:
                    requested_amount = float(order["amount"])
            except (TypeError, ValueError):
                requested_amount = amount

            try:
                average_price = float(order.get("average")) if order.get("average") is not None else None
            except (TypeError, ValueError):
                average_price = None

            norm = self._normalize_order_status(
                raw_status,
                filled=filled,
                amount=requested_amount,
            )

            if norm == "error" and raw_status:
                logger.warning(
                    f"⚠️ Проблемный статус [{exchange_name}] {raw_status}: {order}"
                )

            if norm == "open":
                logger.warning(
                    f"⚠️ MARKET ORDER не полностью исполнен [{exchange_name}] "
                    f"{side.upper()} {requested_amount} {p_symbol}, "
                    f"filled={filled}, status={raw_status}"
                )

            logger.bind(TRADE=True).info(
                f"🟩 ORDER {exchange_name} {side.upper()} {requested_amount} {p_symbol} "
                f"({ccxt_symbol}) → {norm} (filled={filled}, avg_price={average_price})"
            )

            return {
                "status": norm,
                "data": order,
                "msg": None,
                "filled": filled,
                "requested_amount": requested_amount,
                "average_price": average_price,
            }

        except Exception as e:
            code = self._classify_exception(e)
            health.record_request(False, code)
            
            if code == "rate_limit":
                health.rate_limit_hits += 1

            logger.error(
                f"❌ ORDER ERR [{exchange_name}] {side.upper()} {amount} {p_symbol}: "
                f"{e} | code={code}"
            )
            return {
                "status": "error",
                "data": None,
                "msg": code,
                "filled": None,
                "requested_amount": amount,
                "average_price": None,
            }

    # ============================================================
    # BALANCE
    # ============================================================

    async def fetch_balance(self, exchange_name: str) -> Optional[Dict[str, Any]]:
        """Получить баланс аккаунта на бирже."""
        exchange = await self.load_exchange(exchange_name)
        if not exchange:
            return None

        name = self._normalize_name(exchange_name)
        health = self._get_health(name)

        try:
            balance = await exchange.fetch_balance()
            health.record_request(True)
            return balance
        except Exception as e:
            code = self._classify_exception(e)
            health.record_request(False, code)
            
            if code == "rate_limit":
                health.rate_limit_hits += 1
                
            logger.error(f"❌ fetch_balance({exchange_name}): {e} | code={code}")
            return None

    async def get_free_balance(self, exchange_name: str, currency: str) -> Optional[float]:
        """Получить свободный баланс по валюте."""
        bal = await self.fetch_balance(exchange_name)
        if not bal:
            return None

        try:
            free = bal.get("free") or {}
            return float(free.get(currency, 0.0))
        except Exception as e:
            logger.error(f"❌ get_free_balance({exchange_name}, {currency}): {e}")
            return None

    async def get_total_balance(self, exchange_name: str, currency: str) -> Optional[float]:
        """Получить общий баланс по валюте (free + used)."""
        bal = await self.fetch_balance(exchange_name)
        if not bal:
            return None

        try:
            total = bal.get("total") or {}
            return float(total.get(currency, 0.0))
        except Exception as e:
            logger.error(f"❌ get_total_balance({exchange_name}, {currency}): {e}")
            return None

    async def get_position(self, exchange_name: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get position info for a symbol.
        Returns dict with keys: contracts, side, entryPrice, etc.
        Returns None if no position or error.
        """
        exchange = await self.load_exchange(exchange_name)
        if not exchange:
            return None

        name = self._normalize_name(exchange_name)
        health = self._get_health(name)
        ccxt_symbol = to_ccxt_symbol(exchange_name, symbol)

        try:
            positions = await exchange.fetch_positions([ccxt_symbol])
            health.record_request(True)

            if not positions:
                return None

            for pos in positions:
                if pos.get("symbol") == ccxt_symbol:
                    contracts = pos.get("contracts") or pos.get("contractSize") or 0
                    if contracts and float(contracts) != 0:
                        return pos

            return None
        except Exception as e:
            code = self._classify_exception(e)
            health.record_request(False, code)

            if code == "rate_limit":
                health.rate_limit_hits += 1

            logger.error(f"❌ get_position({exchange_name}, {ccxt_symbol}): {e} | code={code}")
            return None

    async def set_leverage(self, exchange_name: str, symbol: str, leverage: int) -> bool:
        """
        Set leverage for a symbol on exchange.
        Returns True if successful, False otherwise.
        """
        exchange = await self.load_exchange(exchange_name)
        if not exchange:
            return False

        name = self._normalize_name(exchange_name)
        health = self._get_health(name)
        ccxt_symbol = to_ccxt_symbol(exchange_name, symbol)

        try:
            await exchange.set_leverage(leverage, ccxt_symbol)
            health.record_request(True)
            logger.info(f"✅ Leverage set [{exchange_name}] {symbol} = {leverage}x")
            return True
        except Exception as e:
            code = self._classify_exception(e)
            health.record_request(False, code)

            # Some exchanges don't support set_leverage or it's already set
            error_msg = str(e).lower()
            if "not support" in error_msg or "already" in error_msg or "same" in error_msg:
                logger.debug(f"⚠️ set_leverage skipped [{exchange_name}] {symbol}: {e}")
                return True  # Consider it OK

            if code == "rate_limit":
                health.rate_limit_hits += 1

            logger.warning(f"⚠️ set_leverage failed [{exchange_name}] {symbol}: {e} | code={code}")
            return False

    # ============================================================
    # HEALTH & METRICS
    # ============================================================

    def get_health(self, exchange_name: str) -> Optional[dict]:
        """Получить метрики здоровья биржи."""
        name = self._normalize_name(exchange_name)
        health = self._health.get(name)
        if health:
            return health.to_dict()
        return None

    def get_all_health(self) -> Dict[str, dict]:
        """Получить метрики здоровья всех бирж."""
        return {name: h.to_dict() for name, h in self._health.items()}

    def is_healthy(self, exchange_name: str) -> bool:
        """Проверить здоровье соединения с биржей."""
        name = self._normalize_name(exchange_name)
        health = self._health.get(name)
        if not health:
            return False
        return health.is_healthy

    # ============================================================
    # CACHE MANAGEMENT
    # ============================================================

    def clear_market_info_cache(self):
        """Очистить кэш market info."""
        self._market_info_cache.clear()

    def cleanup_stale_cache(self):
        """Удалить устаревшие записи из кэша."""
        stale_keys = [
            key for key, cached in self._market_info_cache.items()
            if not cached.is_valid()
        ]
        for key in stale_keys:
            del self._market_info_cache[key]

    # ============================================================
    # CLOSE
    # ============================================================

    async def close_all(self):
        """Аккуратно закрыть все активные соединения с биржами."""
        for name, ex in list(self.active_exchanges.items()):
            try:
                await ex.close()
                logger.debug(f"🛑 Закрыто соединение с {name}")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка закрытия соединения {name}: {e}")
            
            # Обновляем health
            health = self._health.get(name)
            if health:
                health.connected = False
                
        self.active_exchanges.clear()

    async def reconnect(self, exchange_name: str) -> bool:
        """
        Переподключиться к бирже.
        Полезно после серии ошибок.
        """
        name = self._normalize_name(exchange_name)
        
        # Закрываем существующее соединение
        existing = self.active_exchanges.pop(name, None)
        if existing:
            try:
                await existing.close()
            except Exception:
                pass

        # Очищаем кэш для этой биржи
        keys_to_remove = [k for k in self._market_info_cache if k.startswith(f"{name}:")]
        for key in keys_to_remove:
            del self._market_info_cache[key]

        # Пробуем подключиться заново
        inst = await self.load_exchange(name)
        return inst is not None
