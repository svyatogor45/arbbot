# trade_engine.py
# ---------------------------------------------------
# Торговый модуль: единая точка входа/выхода ордеров.
#
# ВАЖНО:
#   - TradeEngine НИЧЕГО не решает про направление арбитража.
#     Он лишь исполняет сигнал:
#       signal["buy_exchange"]  → LONG (BUY)
#       signal["sell_exchange"] → SHORT (SELL)
#   - Выбор лучшей связки бирж и направления теперь лежит
#     на MarketEngine (find_best_opportunity / check_spread).
#
# ИСПРАВЛЕНИЯ v2:
#   - execute_exit() поддерживает оба формата ключей
#   - execute_exit() принимает volume как отдельный параметр
#   - pair_id передаётся в save_emergency_position()
#   - Убраны await перед синхронными методами DB
# ---------------------------------------------------
import asyncio
import uuid
import time
from typing import Optional, Dict, Any, TypedDict, Tuple

from loguru import logger
from exchange_manager import ExchangeManager
from db_manager import DBManager
from symbol_mapper import normalize_base_quote
from config import CRITICAL_IMBALANCE_PCT, WARNING_IMBALANCE_PCT


# ============================================================
# КОНФИГУРАЦИЯ RETRY / BACKOFF
# ============================================================

# Базовая задержка для exponential backoff (секунды)
BASE_RETRY_DELAY = 0.1  # FIX 3.2: было 0.5, ускорено для арбитража

# Максимальная задержка между ретраями (секунды)
MAX_RETRY_DELAY = 10.0

# Множитель для exponential backoff
BACKOFF_MULTIPLIER = 2.0

# Минимальный порог исполнения (% от запрошенного объёма)
# Если исполнено меньше — считаем ордер проблемным
MIN_FILL_RATIO = 0.95  # 95%

# Порог для предупреждения о частичном исполнении
PARTIAL_FILL_WARNING_RATIO = 0.99  # 99%

# ============================================================
# FIX Problem 2: КОНФИГУРАЦИЯ EMERGENCY CLOSE
# ============================================================

# Максимальное количество полных циклов emergency close
MAX_EMERGENCY_ATTEMPTS = 3

# Таймаут на весь emergency close цикл (секунды)
EMERGENCY_CLOSE_TIMEOUT = 20.0  # FIX 3.3: было 60, ускорено для быстрой эскалации

# Задержка между emergency попытками (секунды)
EMERGENCY_RETRY_DELAY = 2.0


class OrderResult(TypedDict, total=False):
    """
    Унифицированный формат результата ордера, который возвращает TradeEngine.

    Поля:
      - status: str        — "filled" | "open" | "canceled" | "error" | ...
      - data: dict | None  — сырой ответ из ExchangeManager/CCXT
      - msg: str | None    — текстовое описание ошибки/состояния
      - filled: float | None
      - requested_amount: float
    """
    status: str
    data: Optional[dict]
    msg: Optional[str]
    filled: Optional[float]
    requested_amount: float


class TradeEngine:
    """
    Управляет:
      • входом в арбитраж (execute_entry)
      • выходом из арбитража (execute_exit)

    Задачи:
      - сделать открытие/закрытие ног устойчивым к временным ошибкам;
      - логировать понятные причины отказа;
      - отдавать в main.py компактные, но информативные коды ошибок;
      - контролировать частичное исполнение и дисбаланс ног.
    """

    def __init__(
        self,
        exchange_manager: ExchangeManager,
        db_manager: DBManager,
        retry_attempts: int = 3,
        base_retry_delay: float = BASE_RETRY_DELAY,
    ):
        # ExchangeManager (реальный или бумажный) передаётся снаружи
        self.manager = exchange_manager
        
        # DBManager для сохранения emergency positions
        self.db = db_manager

        # Параметры ретраев для ордеров
        self.retry_attempts = retry_attempts
        self.base_retry_delay = base_retry_delay

        # Cache: (exchange, symbol) pairs where leverage is already set this session
        self._leverage_set: set = set()

    # ============================================================
    # LEVERAGE MANAGEMENT
    # ============================================================

    async def ensure_leverage(self, exchange: str, symbol: str, leverage: int) -> bool:
        """
        Ensure leverage is set for (exchange, symbol).
        Uses cache to avoid repeated calls within same session.
        Returns True if leverage is set (or was already set).
        """
        cache_key = (exchange.lower(), symbol)

        if cache_key in self._leverage_set:
            return True

        success = await self.manager.set_leverage(exchange, symbol, leverage)

        if success:
            self._leverage_set.add(cache_key)

        return success

    # ============================================================
    # ВЫЧИСЛЕНИЕ ЗАДЕРЖКИ (EXPONENTIAL BACKOFF)
    # ============================================================

    def _get_retry_delay(self, attempt: int) -> float:
        """
        Вычислить задержку перед следующей попыткой с exponential backoff.
        
        attempt=1 → base_delay
        attempt=2 → base_delay * 2
        attempt=3 → base_delay * 4
        ...
        Но не больше MAX_RETRY_DELAY
        """
        delay = self.base_retry_delay * (BACKOFF_MULTIPLIER ** (attempt - 1))
        return min(delay, MAX_RETRY_DELAY)

    # ============================================================
    # ПРОВЕРКА МИНИМАЛЬНОГО РАЗМЕРА ОРДЕРА
    # ============================================================

    async def _check_min_order_size(
        self,
        exchange: str,
        symbol: str,
        amount: float,
        price: float,
    ) -> Tuple[bool, str, Optional[float]]:
        """
        Проверить, что ордер соответствует минимальным требованиям биржи.
        
        Возвращает (ok, reason, min_amount).
        """
        try:
            market_info = await self.manager.get_market_info(exchange, symbol)
            if not market_info:
                # Нет информации — пропускаем проверку
                return True, "no_market_info", None
            
            limits = market_info.get("limits", {})
            amount_limits = limits.get("amount", {})
            cost_limits = limits.get("cost", {})
            
            min_amount = amount_limits.get("min")
            min_cost = cost_limits.get("min")
            
            # Проверка минимального количества
            if min_amount is not None and amount < float(min_amount):
                return False, "below_min_amount", float(min_amount)
            
            # Проверка минимальной стоимости (notional)
            if min_cost is not None and price > 0:
                notional = amount * price
                if notional < float(min_cost):
                    return False, "below_min_notional", float(min_cost)
            
            return True, "ok", min_amount
            
        except Exception as e:
            logger.warning(f"⚠️ Ошибка проверки min_order_size [{exchange}]: {e}")
            # При ошибке проверки — пропускаем, пусть биржа сама отклонит
            return True, "check_error", None

    # ============================================================
    # НИЗКОУРОВНЕВЫЙ ХЕЛПЕР ДЛЯ ОРДЕРОВ
    # ============================================================

    def _generate_client_order_id(self, exchange: str, side: str) -> str:
        """
        Генерирует уникальный clientOrderId для дедупликации ордеров.

        Формат: ARB_{exchange}_{side}_{timestamp_ms}_{uuid4_short}
        Пример: ARB_bybit_buy_1701234567890_a1b2c3d4

        Большинство бирж поддерживают clientOrderId до 32-36 символов.
        """
        ts = int(time.time() * 1000)
        short_uuid = uuid.uuid4().hex[:8]
        return f"ARB_{exchange[:4]}_{side}_{ts}_{short_uuid}"

    async def _order(
        self,
        exchange: str,
        symbol: str,
        side: str,
        amount: float,
        params: Optional[dict] = None,
        client_order_id: Optional[str] = None,
    ) -> OrderResult:
        """
        Базовая обёртка над ExchangeManager.place_order.

        Гарантирует:
          - отсутствие исключений снаружи (всё сводим к status="error");
          - единый формат результата OrderResult.
          - FIX #4: clientOrderId для дедупликации ордеров

        side: "buy" | "sell"
        """
        if amount <= 0:
            return OrderResult(
                status="error",
                data=None,
                msg="non_positive_amount",
                filled=None,
                requested_amount=amount,
            )

        # FIX #4: Генерируем clientOrderId если не передан
        order_params = dict(params) if params else {}
        coid = client_order_id or self._generate_client_order_id(exchange, side)
        order_params["clientOrderId"] = coid

        try:
            raw = await self.manager.place_order(
                exchange_name=exchange,
                symbol=symbol,
                side=side,
                amount=amount,
                params=order_params,
            )
        except Exception as e:
            logger.exception(
                f"❌ EXCEPTION in place_order [{exchange}] {symbol} {side} {amount}: {e}"
            )
            return OrderResult(
                status="error",
                data=None,
                msg=f"exception:{e}",
                filled=None,
                requested_amount=amount,
            )

        if not isinstance(raw, dict):
            return OrderResult(
                status="error",
                data=None,
                msg="invalid_response_type",
                filled=None,
                requested_amount=amount,
            )

        # Не портим исходный dict, но забираем основные поля.
        res: OrderResult = OrderResult(
            status=str(raw.get("status", "error")),
            data=raw,
            msg=raw.get("msg"),
            filled=raw.get("filled"),
            requested_amount=amount,
        )
        return res

    async def _order_with_retries(
        self,
        exchange: str,
        symbol: str,
        side: str,
        amount: float,
        leg_label: str,
        params: Optional[dict] = None,
    ) -> OrderResult:
        """
        Выполнить ордер с несколькими попытками и exponential backoff.

        Успехом считаем любой статус, отличающийся от "error".

        FIX #4: clientOrderId генерируется ОДИН раз и используется во всех retry.
        Это гарантирует, что биржа отклонит дубликат, если первый ордер уже исполнился.

        FIX: Перед retry проверяем позицию на бирже — если ордер уже исполнился,
        не делаем повторный (предотвращает удвоение позиции при network timeout).

        Используется для:
          - открытия ног (entry_long / entry_short)
          - аварийного закрытия (emergency_close_long)
          - дожимания ног при выходе (exit_long_retry / exit_short_retry)
        """
        last_result: Optional[OrderResult] = None

        # FIX #4: Генерируем clientOrderId ОДИН раз для всех retry
        # Биржа отклонит повторный ордер с тем же ID если первый уже исполнился
        client_order_id = self._generate_client_order_id(exchange, side)

        # FIX 3.1: Убрана проверка позиции - clientOrderId защищает от дублей

        for attempt in range(1, self.retry_attempts + 1):
            res = await self._order(
                exchange, symbol, side, amount,
                params=params,
                client_order_id=client_order_id,
            )
            last_result = res

            if res["status"] != "error":
                # Проверяем качество исполнения
                filled = res.get("filled")
                if filled is not None and amount > 0:
                    fill_ratio = filled / amount

                    if fill_ratio < PARTIAL_FILL_WARNING_RATIO:
                        logger.warning(
                            f"⚠️ PARTIAL FILL [{exchange}] {symbol} {side} "
                            f"| requested={amount}, filled={filled} "
                            f"| ratio={fill_ratio:.2%}"
                        )

                logger.info(
                    f"✅ ORDER OK [{exchange}] {symbol} {side} {amount} "
                    f"| leg={leg_label} | attempt={attempt} "
                    f"| status={res['status']} filled={res.get('filled')}"
                )
                return res

            # Определяем, стоит ли ретраить
            error_msg = res.get("msg") or ""

            # Некоторые ошибки не имеет смысла ретраить
            non_retryable_errors = [
                "insufficient_funds",
                "invalid_order",
                "below_min",
                "auth_error",
            ]

            is_retryable = not any(err in error_msg.lower() for err in non_retryable_errors)

            if not is_retryable:
                logger.error(
                    f"🛑 ORDER FAILED (non-retryable) [{exchange}] {symbol} {side} {amount} "
                    f"| leg={leg_label} | msg={error_msg}"
                )
                return res

            # FIX 3.1: Убрана проверка позиции перед retry - clientOrderId защищает от дублей
            if attempt < self.retry_attempts:
                delay = self._get_retry_delay(attempt)
                logger.warning(
                    f"🔁 ORDER RETRY {attempt}/{self.retry_attempts} "
                    f"[{exchange}] {symbol} {side} {amount} | leg={leg_label} "
                    f"| msg={error_msg} | next_delay={delay:.2f}s"
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"🛑 ORDER FAILED after {self.retry_attempts} attempts "
                    f"[{exchange}] {symbol} {side} {amount} | leg={leg_label} "
                    f"| last_msg={error_msg}"
                )

        # Возвращаем последний результат
        return last_result or OrderResult(
            status="error",
            data=None,
            msg="all_retries_exhausted",
            filled=None,
            requested_amount=amount,
        )

    # ============================================================
    # ДОКУПКА НЕДОСТАЮЩЕГО ОБЪЁМА
    # ============================================================

    async def _fill_remaining(
        self,
        exchange: str,
        symbol: str,
        side: str,
        target_amount: float,
        already_filled: float,
        leg_label: str,
        params: Optional[dict] = None,
    ) -> OrderResult:
        """
        Докупить недостающий объём до target_amount.
        
        Возвращает итоговый OrderResult с суммарным filled.
        """
        remaining = target_amount - already_filled
        
        if remaining <= 0:
            # Уже исполнено достаточно
            return OrderResult(
                status="filled",
                data=None,
                msg="already_filled",
                filled=already_filled,
                requested_amount=target_amount,
            )
        
        # Проверяем, что remaining не слишком мал для биржи
        # (упрощённая проверка — в идеале нужно знать min_amount)
        if remaining < target_amount * 0.01:  # меньше 1% от оригинала
            logger.info(
                f"📊 Remaining too small to fill [{exchange}] {symbol} "
                f"| remaining={remaining} | considering filled"
            )
            return OrderResult(
                status="filled",
                data=None,
                msg="remaining_too_small",
                filled=already_filled,
                requested_amount=target_amount,
            )
        
        logger.info(
            f"📊 FILL REMAINING [{exchange}] {symbol} {side} "
            f"| target={target_amount}, filled={already_filled}, remaining={remaining}"
        )
        
        fill_order = await self._order_with_retries(
            exchange=exchange,
            symbol=symbol,
            side=side,
            amount=remaining,
            leg_label=f"{leg_label}_fill_remaining",
            params=params,
        )
        
        additional_filled = fill_order.get("filled") or 0.0
        total_filled = already_filled + additional_filled
        
        # Определяем итоговый статус
        if fill_order["status"] == "error":
            # Частично исполнено, но докупка не удалась
            return OrderResult(
                status="partial" if already_filled > 0 else "error",
                data=fill_order.get("data"),
                msg=f"fill_remaining_failed:{fill_order.get('msg')}",
                filled=total_filled,
                requested_amount=target_amount,
            )
        
        return OrderResult(
            status="filled" if total_filled >= target_amount * MIN_FILL_RATIO else "partial",
            data=fill_order.get("data"),
            msg=None,
            filled=total_filled,
            requested_amount=target_amount,
        )

    # ============================================================
    # ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА БАЛАНСА (P0)
    # ============================================================

    async def _check_balance(self, exchange: str, asset: str, required: float) -> bool:
        """
        Проверка, хватает ли на бирже средств под вход (P0).

        В текущей реализации для линейных USDT-фьючерсов:
          - проверяем только USDT как маржинальный актив на нужной бирже.

        В дальнейшем можно заменить на полноценную проверку свободной маржи
        на фьючерсном аккаунте.
        """
        if required <= 0:
            return True  # ничего не нужно — формально считаем, что хватает

        free = await self.manager.get_free_balance(exchange, asset)
        if free is None:
            logger.error(f"⚠ Баланс не получен [{exchange}] для {asset}")
            return False

        if free < required:
            logger.warning(
                f"⚠ Недостаточно баланса [{exchange}] {asset} | "
                f"есть={free}, требуется={required}"
            )
            return False

        return True

    # ============================================================
    # АВАРИЙНОЕ ЗАКРЫТИЕ НОГИ (ИСПРАВЛЕНО: pair_id + sync DB)
    # ============================================================

    async def _emergency_close_leg(
        self,
        exchange: str,
        symbol: str,
        side: str,
        amount: float,
        leg_label: str,
        pair_id: Optional[int] = None,
    ) -> dict:
        """
        Экстренное закрытие ноги с ретраями, таймаутом и эскалацией.

        FIX Problem 2: Добавлены:
        - MAX_EMERGENCY_ATTEMPTS — общий лимит попыток
        - EMERGENCY_CLOSE_TIMEOUT — таймаут на весь цикл
        - Эскалация при достижении лимитов

        Args:
            exchange: Название биржи
            symbol: Торговая пара
            side: "buy" или "sell"
            amount: Объём для закрытия
            leg_label: Метка для логов
            pair_id: ID пары (для записи в emergency_positions)

        Возвращает:
        {
            "success": bool,
            "order": OrderResult,
            "critical": bool,  # True если не удалось закрыть
            "escalated": bool  # True если достигнут лимит попыток
        }
        """
        import time
        start_time = time.time()

        logger.warning(
            f"🚨 EMERGENCY CLOSE [{exchange}] {symbol} {side} {amount} | {leg_label}"
        )

        last_order: Optional[OrderResult] = None
        total_filled = 0.0
        remaining_amount = amount

        for attempt in range(1, MAX_EMERGENCY_ATTEMPTS + 1):
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > EMERGENCY_CLOSE_TIMEOUT:
                logger.critical(
                    f"💀 EMERGENCY CLOSE TIMEOUT [{exchange}] {symbol} {side} | "
                    f"elapsed={elapsed:.1f}s > {EMERGENCY_CLOSE_TIMEOUT}s | "
                    f"filled={total_filled}/{amount}"
                )
                break

            logger.info(
                f"🔄 EMERGENCY CLOSE attempt {attempt}/{MAX_EMERGENCY_ATTEMPTS} "
                f"[{exchange}] {symbol} {side} {remaining_amount:.6f}"
            )

            close_order = await self._order_with_retries(
                exchange=exchange,
                symbol=symbol,
                side=side,
                amount=remaining_amount,
                leg_label=f"{leg_label}_attempt{attempt}",
                params={"reduceOnly": True},
            )
            last_order = close_order

            if close_order["status"] != "error":
                filled = close_order.get("filled") or 0.0
                total_filled += filled
                remaining_amount = amount - total_filled

                # Check if fully closed
                if total_filled >= amount * MIN_FILL_RATIO:
                    logger.info(
                        f"✅ EMERGENCY CLOSE OK [{exchange}] {symbol} {side} | "
                        f"filled={total_filled}/{amount} | attempts={attempt}"
                    )
                    return {
                        "success": True,
                        "order": close_order,
                        "critical": False,
                        "escalated": False,
                    }

                # Partially filled - continue trying
                logger.warning(
                    f"⚠️ EMERGENCY CLOSE PARTIAL [{exchange}] {symbol} | "
                    f"filled={total_filled}/{amount}, remaining={remaining_amount}"
                )
            else:
                logger.error(
                    f"❌ EMERGENCY CLOSE attempt {attempt} FAILED | "
                    f"msg={close_order.get('msg')}"
                )

            # Wait before next attempt (unless last attempt)
            if attempt < MAX_EMERGENCY_ATTEMPTS:
                await asyncio.sleep(EMERGENCY_RETRY_DELAY)

        # All attempts exhausted or timeout
        logger.critical(
            f"💀 EMERGENCY CLOSE ESCALATION [{exchange}] {symbol} {side} | "
            f"All {MAX_EMERGENCY_ATTEMPTS} attempts failed | "
            f"filled={total_filled}/{amount} | pair_id={pair_id}"
        )

        # Save remaining position to DB for manual intervention
        if remaining_amount > 0:
            await self.db.save_emergency_position(
                pair_id=pair_id or 0,
                exchange=exchange,
                symbol=symbol,
                side="long" if side == "sell" else "short",
                amount=remaining_amount,
                reason=f"emergency_close_escalated:attempts={MAX_EMERGENCY_ATTEMPTS},filled={total_filled}",
            )

        return {
            "success": total_filled > 0,
            "order": last_order or OrderResult(
                status="error",
                data=None,
                msg="all_emergency_attempts_failed",
                filled=total_filled,
                requested_amount=amount,
            ),
            "critical": True,
            "escalated": True,
        }

    # ============================================================
    # ВХОД В ПОЗИЦИЮ (LONG + SHORT)
    # ============================================================

    async def execute_entry(
        self,
        signal: dict,
        volume: float,
        pair_id: Optional[int] = None,
        leverage: int = 10,
    ) -> dict:
        """
        Открываем две ноги:
          1) LONG: BUY  на signal["buy_exchange"]
          2) SHORT: SELL на signal["sell_exchange"]

        ВАЖНО:
          - TradeEngine не решает, где дешевле/дороже.
            Он доверяет тому, что передал MarketEngine.
          - symbol, buy_exchange, sell_exchange, buy_price, sell_price
            должны быть уже подготовлены в signal.

        Args:
            signal: Сигнал от MarketEngine
            volume: Объём для входа
            pair_id: ID пары (для записи в emergency_positions)
            leverage: Плечо для обеих ног (default 10)

        Возврат:
        {
            "success": bool,
            "entry_long_order": OrderResult | None,
            "entry_short_order": OrderResult | None,
            "error": str | None,
            "imbalance": float | None  # разница между filled LONG и SHORT
        }

        Возможные error:
          - "non_positive_volume"
          - "missing_prices_in_signal"
          - "insufficient_balance"
          - "same_exchange_for_both_legs"
          - "below_min_order_size"
          - "long_leg_failed"
          - "second_leg_failed_emergency_close"
          - "critical_imbalance"
        """

        symbol = signal["symbol"]
        long_ex = signal["buy_exchange"]    # тут открываем LONG
        short_ex = signal["sell_exchange"]  # тут открываем SHORT

        if long_ex == short_ex:
            logger.error(
                f"❌ ENTRY FAILED {symbol} | buy_exchange == sell_exchange == {long_ex}"
            )
            return {
                "success": False,
                "entry_long_order": None,
                "entry_short_order": None,
                "error": "same_exchange_for_both_legs",
                "imbalance": None,
            }

        if volume <= 0:
            logger.error(f"❌ ENTRY FAILED {symbol} | non-positive volume={volume}")
            return {
                "success": False,
                "entry_long_order": None,
                "entry_short_order": None,
                "error": "non_positive_volume",
                "imbalance": None,
            }

        buy_price = signal.get("buy_price")
        sell_price = signal.get("sell_price")

        if buy_price is None or sell_price is None:
            logger.error(
                f"❌ ENTRY FAILED {symbol} | нет buy_price/sell_price в signal: {signal}"
            )
            return {
                "success": False,
                "entry_long_order": None,
                "entry_short_order": None,
                "error": "missing_prices_in_signal",
                "imbalance": None,
            }

        # Нормализуем символ (BTC/USDT, BTCUSDT, BTC-USDT-SWAP и т.п.)
        try:
            base, quote = normalize_base_quote(symbol)
            logger.debug(
                f"🔎 NORMALIZED SYMBOL {symbol} -> base={base}, quote={quote}"
            )
        except Exception as e:
            logger.warning(
                f"⚠ Не удалось нормализовать символ {symbol}: {e}. "
                f"Будем считать quote=USDT для проверки маржи."
            )
            base, quote = symbol, "USDT"

        # ------------------------------------------------------------
        # FIX #2: Double Entry Prevention - проверка существующих позиций
        # ------------------------------------------------------------
        try:
            long_position = await self.manager.get_position(long_ex, symbol)
            short_position = await self.manager.get_position(short_ex, symbol)

            long_contracts = abs(float(long_position.get("contracts", 0))) if long_position else 0.0
            short_contracts = abs(float(short_position.get("contracts", 0))) if short_position else 0.0

            if long_contracts > 0 or short_contracts > 0:
                logger.error(
                    f"❌ ENTRY BLOCKED {symbol} | Позиции уже существуют: "
                    f"[{long_ex}] LONG={long_contracts:.6f}, [{short_ex}] SHORT={short_contracts:.6f}"
                )
                return {
                    "success": False,
                    "entry_long_order": None,
                    "entry_short_order": None,
                    "error": "existing_positions_detected",
                    "imbalance": None,
                }
        except Exception as e:
            logger.warning(f"⚠ Не удалось проверить позиции перед входом: {e}. Продолжаем.")

        # ------------------------------------------------------------
        # Проверка минимального размера ордера
        # ------------------------------------------------------------
        min_ok_long, min_reason_long, min_amount_long = await self._check_min_order_size(
            long_ex, symbol, volume, buy_price
        )
        min_ok_short, min_reason_short, min_amount_short = await self._check_min_order_size(
            short_ex, symbol, volume, sell_price
        )
        
        if not min_ok_long:
            logger.error(
                f"❌ ENTRY FAILED {symbol} | LONG below min: {min_reason_long}, "
                f"min_amount={min_amount_long}, requested={volume}"
            )
            return {
                "success": False,
                "entry_long_order": None,
                "entry_short_order": None,
                "error": f"below_min_order_size_long:{min_reason_long}",
                "imbalance": None,
            }
        
        if not min_ok_short:
            logger.error(
                f"❌ ENTRY FAILED {symbol} | SHORT below min: {min_reason_short}, "
                f"min_amount={min_amount_short}, requested={volume}"
            )
            return {
                "success": False,
                "entry_long_order": None,
                "entry_short_order": None,
                "error": f"below_min_order_size_short:{min_reason_short}",
                "imbalance": None,
            }

        # ------------------------------------------------------------
        # P0 для линейных USDT-фьючерсов:
        #   - считаем, что маржа в USDT по обеим ногам
        #   - проверяем только USDT на обеих биржах
        # ------------------------------------------------------------
        margin_asset = "USDT"

        required_quote_for_long = volume * buy_price
        required_quote_for_short = volume * sell_price

        ok_long = await self._check_balance(
            long_ex,
            margin_asset,
            required_quote_for_long,
        )
        ok_short = await self._check_balance(
            short_ex,
            margin_asset,
            required_quote_for_short,
        )

        if not (ok_long and ok_short):
            logger.error(
                "❌ ENTRY FAILED | недостаточно средств на биржах под маржу: "
                f"{long_ex} (LONG, требуется≈{required_quote_for_long} {margin_asset}), "
                f"{short_ex} (SHORT, требуется≈{required_quote_for_short} {margin_asset})"
            )
            return {
                "success": False,
                "entry_long_order": None,
                "entry_short_order": None,
                "error": "insufficient_balance",
                "imbalance": None,
            }

        # ------------------------------------------------------------
        # SET LEVERAGE (cached - only once per session per exchange+symbol)
        # ------------------------------------------------------------
        await asyncio.gather(
            self.ensure_leverage(long_ex, symbol, leverage),
            self.ensure_leverage(short_ex, symbol, leverage),
        )

        logger.info(
            f"🚀 ENTRY TRY (PARALLEL) | LONG [{long_ex}] BUY {volume} | "
            f"SHORT [{short_ex}] SELL {volume} | {symbol} | leverage={leverage}x"
        )

        # ------------------------------------------------------------
        # ПАРАЛЛЕЛЬНОЕ ОТКРЫТИЕ ОБЕИХ НОГ
        # ------------------------------------------------------------
        long_task = self._order_with_retries(
            long_ex,
            symbol,
            "buy",
            volume,
            leg_label="entry_long",
            params={"reduceOnly": False},
        )

        short_task = self._order_with_retries(
            short_ex,
            symbol,
            "sell",
            volume,
            leg_label="entry_short",
            params={"reduceOnly": False},
        )

        long_order, short_order = await asyncio.gather(long_task, short_task)

        long_success = long_order["status"] != "error"
        short_success = short_order["status"] != "error"
        long_filled = long_order.get("filled") or 0.0
        short_filled = short_order.get("filled") or 0.0

        # ------------------------------------------------------------
        # CASE 1: Обе ноги failed
        # ------------------------------------------------------------
        if not long_success and not short_success:
            logger.error(
                f"❌ ENTRY FAILED | Both legs failed | "
                f"LONG: {long_order.get('msg')} | SHORT: {short_order.get('msg')}"
            )
            return {
                "success": False,
                "entry_long_order": long_order,
                "entry_short_order": short_order,
                "error": "both_legs_failed",
                "imbalance": None,
            }

        # ------------------------------------------------------------
        # CASE 2: Только LONG failed → закрыть SHORT
        # ------------------------------------------------------------
        if not long_success and short_success:
            logger.error(
                f"❌ LONG FAILED | {long_ex} | {long_order.get('msg')} | "
                f"Emergency closing SHORT"
            )

            if short_filled > 0:
                close_result = await self._emergency_close_leg(
                    exchange=short_ex,
                    symbol=symbol,
                    side="buy",
                    amount=short_filled,
                    leg_label="emergency_close_short",
                    pair_id=pair_id,
                )

                if close_result["critical"]:
                    logger.critical(
                        f"💀 EMERGENCY CLOSE SHORT FAILED | Position saved to DB"
                    )

            return {
                "success": False,
                "entry_long_order": long_order,
                "entry_short_order": short_order,
                "error": "long_leg_failed_emergency_close",
                "imbalance": None,
            }

        # ------------------------------------------------------------
        # CASE 3: Только SHORT failed → закрыть LONG
        # ------------------------------------------------------------
        if long_success and not short_success:
            logger.error(
                f"❌ SHORT FAILED | {short_ex} | {short_order.get('msg')} | "
                f"Emergency closing LONG"
            )

            if long_filled > 0:
                close_result = await self._emergency_close_leg(
                    exchange=long_ex,
                    symbol=symbol,
                    side="sell",
                    amount=long_filled,
                    leg_label="emergency_close_long",
                    pair_id=pair_id,
                )

                if close_result["critical"]:
                    logger.critical(
                        f"💀 EMERGENCY CLOSE LONG FAILED | Position saved to DB"
                    )

            return {
                "success": False,
                "entry_long_order": long_order,
                "entry_short_order": short_order,
                "error": "short_leg_failed_emergency_close",
                "imbalance": None,
            }

        # ------------------------------------------------------------
        # CASE 4: Обе ноги успешны → проверяем fill и балансируем
        # ------------------------------------------------------------

        # Докупаем LONG если частично исполнен
        if long_filled < volume * MIN_FILL_RATIO:
            logger.warning(
                f"⚠️ LONG частично исполнен [{long_ex}] | "
                f"filled={long_filled}, requested={volume}"
            )
            long_order = await self._fill_remaining(
                long_ex, symbol, "buy", volume, long_filled,
                leg_label="entry_long",
                params={"reduceOnly": False},
            )
            long_filled = long_order.get("filled") or long_filled

        # Докупаем SHORT если частично исполнен
        if short_filled < volume * MIN_FILL_RATIO:
            logger.warning(
                f"⚠️ SHORT частично исполнен [{short_ex}] | "
                f"filled={short_filled}, requested={volume}"
            )
            short_order = await self._fill_remaining(
                short_ex, symbol, "sell", volume, short_filled,
                leg_label="entry_short",
                params={"reduceOnly": False},
            )
            short_filled = short_order.get("filled") or short_filled

        # Вычисляем дисбаланс
        imbalance = long_filled - short_filled
        imbalance_pct = abs(imbalance) / volume * 100 if volume > 0 else 0

        # Проверяем пороги дисбаланса
        if imbalance_pct > WARNING_IMBALANCE_PCT:
            logger.warning(
                f"⚠️ ENTRY IMBALANCE WARNING | LONG={long_filled}, SHORT={short_filled}, "
                f"diff={imbalance:.6f} ({imbalance_pct:.2f}%)"
            )

        # Критичный дисбаланс — требуется действие
        if imbalance_pct > CRITICAL_IMBALANCE_PCT:
            logger.error(
                f"🚨 CRITICAL IMBALANCE DETECTED | "
                f"LONG={long_filled}, SHORT={short_filled}, "
                f"diff={imbalance:.6f} ({imbalance_pct:.2f}%) > {CRITICAL_IMBALANCE_PCT}%"
            )

            # Определяем, какая нога в избытке
            excess_amount = abs(imbalance)

            if imbalance > 0:
                # LONG в избытке — закрываем лишний LONG
                excess_exchange = long_ex
                excess_side = "sell"
                excess_leg = "LONG"
            else:
                # SHORT в избытке — закрываем лишний SHORT
                excess_exchange = short_ex
                excess_side = "buy"
                excess_leg = "SHORT"

            logger.warning(
                f"🔧 CLOSING EXCESS {excess_leg} | [{excess_exchange}] {excess_side} {excess_amount:.6f}"
            )

            close_result = await self._emergency_close_leg(
                exchange=excess_exchange,
                symbol=symbol,
                side=excess_side,
                amount=excess_amount,
                leg_label=f"close_excess_{excess_leg.lower()}",
                pair_id=pair_id,
            )

            if close_result["critical"]:
                logger.critical(
                    f"💀 FAILED TO CLOSE EXCESS {excess_leg} | "
                    f"Emergency position saved to DB"
                )
                return {
                    "success": False,
                    "entry_long_order": long_order,
                    "entry_short_order": short_order,
                    "error": "critical_imbalance",
                    "imbalance": imbalance,
                }

        # Успешный вход
        logger.info(
            f"ENTRY SUCCESS (PARALLEL) | LONG={long_filled}, SHORT={short_filled}, "
            f"imbalance={imbalance:.6f}"
        )

        return {
            "success": True,
            "entry_long_order": long_order,
            "entry_short_order": short_order,
            "error": None,
            "imbalance": imbalance,
        }

    # ============================================================
    # ВЫХОД ИЗ ПОЗИЦИИ (ИСПРАВЛЕНО: гибкая сигнатура)
    # ============================================================

    async def execute_exit(
        self, 
        position: dict, 
        volume: Optional[float] = None,
    ) -> dict:
        """
        Закрываем открытую арбитражную позицию.

        ИСПРАВЛЕНО: Поддерживает два формата вызова:
        
        Формат 1 (из main.py):
            position = {
                "symbol": str,
                "buy_exchange": str,   # = long_exchange
                "sell_exchange": str,  # = short_exchange
            }
            volume = float  # передаётся отдельно
        
        Формат 2 (полный):
            position = {
                "symbol": str,
                "long_exchange": str,
                "short_exchange": str,
                "long_amount": float,
                "short_amount": float,
                "pair_id": int (опционально)
            }
            volume = None  # берётся из position

        Args:
            position: Словарь с данными позиции
            volume: Объём для закрытия (если не указан в position)

        Возврат:
        {
            "success": bool,
            "exit_long_order": OrderResult | None,
            "exit_short_order": OrderResult | None,
            "error": str | None
        }
        """
        symbol = position["symbol"]
        
        # ИСПРАВЛЕНО: поддержка обоих форматов ключей
        # Приоритет: long_exchange > buy_exchange
        long_ex = position.get("long_exchange") or position.get("buy_exchange")
        short_ex = position.get("short_exchange") or position.get("sell_exchange")
        
        if not long_ex or not short_ex:
            logger.error(
                f"❌ EXIT FAILED {symbol} | Не указаны биржи: "
                f"long_ex={long_ex}, short_ex={short_ex}"
            )
            return {
                "success": False,
                "exit_long_order": None,
                "exit_short_order": None,
                "error": "missing_exchange_info",
            }
        
        # ИСПРАВЛЕНО v2: определяем объёмы с учётом actual volumes (для корректного закрытия при дисбалансе)
        # Приоритет: actual_*_volume > явный volume > long_amount/short_amount из position
        actual_long = position.get("actual_long_volume")
        actual_short = position.get("actual_short_volume")

        if actual_long is not None and actual_long > 0 and actual_short is not None and actual_short > 0:
            # FIX Problem 5: Используем реальные объёмы вместо расчётных
            long_amount = actual_long
            short_amount = actual_short
            logger.debug(
                f"EXIT using ACTUAL volumes | LONG={long_amount}, SHORT={short_amount}"
            )
        elif volume is not None and volume > 0:
            long_amount = volume
            short_amount = volume
        else:
            long_amount = position.get("long_amount") or position.get("volume") or 0.0
            short_amount = position.get("short_amount") or position.get("volume") or 0.0
        
        if long_amount <= 0 or short_amount <= 0:
            logger.error(
                f"❌ EXIT FAILED {symbol} | Некорректные объёмы: "
                f"long_amount={long_amount}, short_amount={short_amount}"
            )
            return {
                "success": False,
                "exit_long_order": None,
                "exit_short_order": None,
                "error": "invalid_volume",
            }
        
        # pair_id для emergency positions
        pair_id = position.get("pair_id")

        logger.info(
            f"🔚 EXIT TRY | LONG [{long_ex}] SELL {long_amount} | "
            f"SHORT [{short_ex}] BUY {short_amount} | {symbol}"
        )

        # Закрываем обе ноги параллельно
        long_task = self._order_with_retries(
            long_ex,
            symbol,
            "sell",
            long_amount,
            leg_label="exit_long",
            params={"reduceOnly": True},
        )
        
        short_task = self._order_with_retries(
            short_ex,
            symbol,
            "buy",
            short_amount,
            leg_label="exit_short",
            params={"reduceOnly": True},
        )

        long_order, short_order = await asyncio.gather(long_task, short_task)

        # Проверяем результаты
        long_success = long_order["status"] != "error"
        short_success = short_order["status"] != "error"

        if long_success and short_success:
            long_filled = long_order.get("filled") or 0.0
            short_filled = short_order.get("filled") or 0.0

            # FIX #6: Проверяем дисбаланс при exit
            imbalance = long_filled - short_filled
            max_expected = max(long_amount, short_amount)
            imbalance_pct = abs(imbalance) / max_expected * 100 if max_expected > 0 else 0

            if imbalance_pct > WARNING_IMBALANCE_PCT:
                logger.warning(
                    f"⚠️ EXIT IMBALANCE WARNING | LONG closed={long_filled}, SHORT closed={short_filled}, "
                    f"diff={imbalance:.6f} ({imbalance_pct:.2f}%)"
                )

            # FIX #6: Критичный дисбаланс — выравниваем
            if imbalance_pct > CRITICAL_IMBALANCE_PCT:
                logger.error(
                    f"🚨 EXIT CRITICAL IMBALANCE | "
                    f"LONG closed={long_filled}, SHORT closed={short_filled}, "
                    f"diff={imbalance:.6f} ({imbalance_pct:.2f}%) > {CRITICAL_IMBALANCE_PCT}%"
                )

                # Определяем "хвост" — какая нога закрылась меньше
                residual_amount = abs(imbalance)

                if imbalance > 0:
                    # LONG закрыт больше, чем SHORT → SHORT недозакрыт
                    # Нужно докупить SHORT (buy) на short_ex
                    residual_exchange = short_ex
                    residual_side = "buy"
                    residual_leg = "SHORT"
                else:
                    # SHORT закрыт больше, чем LONG → LONG недозакрыт
                    # Нужно допродать LONG (sell) на long_ex
                    residual_exchange = long_ex
                    residual_side = "sell"
                    residual_leg = "LONG"

                logger.warning(
                    f"🔧 CLOSING EXIT RESIDUAL {residual_leg} | [{residual_exchange}] {residual_side} {residual_amount:.6f}"
                )

                close_result = await self._emergency_close_leg(
                    exchange=residual_exchange,
                    symbol=symbol,
                    side=residual_side,
                    amount=residual_amount,
                    leg_label=f"exit_residual_{residual_leg.lower()}",
                    pair_id=pair_id,
                )

                if close_result["critical"]:
                    logger.critical(
                        f"💀 EXIT RESIDUAL CLOSE FAILED | {residual_leg} {residual_amount} on {residual_exchange} | saved to emergency"
                    )
                else:
                    logger.info(
                        f"✅ EXIT RESIDUAL CLOSED | {residual_leg} {residual_amount} on {residual_exchange}"
                    )

            logger.info(
                f"EXIT SUCCESS | LONG closed={long_filled}, SHORT closed={short_filled}"
            )

            return {
                "success": True,
                "exit_long_order": long_order,
                "exit_short_order": short_order,
                "error": None,
            }

        # Одна или обе ноги не закрылись
        error_parts = []
        
        if not long_success:
            error_parts.append(f"long_failed:{long_order.get('msg')}")
            # Async DB call
            await self.db.save_emergency_position(
                pair_id=pair_id or 0,
                exchange=long_ex,
                symbol=symbol,
                side="long",
                amount=long_amount,
                reason="exit_long_failed",
            )

        if not short_success:
            error_parts.append(f"short_failed:{short_order.get('msg')}")
            # Async DB call
            await self.db.save_emergency_position(
                pair_id=pair_id or 0,
                exchange=short_ex,
                symbol=symbol,
                side="short",
                amount=short_amount,
                reason="exit_short_failed",
            )
        
        error = "|".join(error_parts)
        
        logger.error(f"❌ EXIT FAILED | {error}")
        
        return {
            "success": False,
            "exit_long_order": long_order,
            "exit_short_order": short_order,
            "error": error,
        }
