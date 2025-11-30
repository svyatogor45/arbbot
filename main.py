# main.py
# ---------------------------------------------------
# EVENT-DRIVEN v4.0 - Pure async architecture
# Performance: 10-50ms reaction, non-blocking DB
# ---------------------------------------------------

import asyncio
import signal
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple, Set

from loguru import logger

from config import (
    PRICE_UPDATE_INTERVAL,
    EXCHANGES,
    MAX_TOTAL_RISK_USDT,
    MAX_PAIR_VOLUME,
    MAX_OPEN_PAIRS,
    MAX_MONITORED_PAIRS,
)

from db_manager import DBManager
from ws_manager import WsManager
from exchange_manager import ExchangeManager
from market_engine import MarketEngine
from trade_engine import TradeEngine

# ==============================
# Константы статусов пары
# ==============================

STATE_READY = "READY"
STATE_ENTERING = "ENTERING"
STATE_HOLD = "HOLD"
STATE_EXITING = "EXITING"
STATE_PAUSED = "PAUSED"
STATE_ERROR = "ERROR"

# Event-driven settings
EVENT_DEBOUNCE_MS = 5  # Минимальный интервал между обработками (оптимизировано: 50ms → 15ms → 5ms)
POSITION_CHECK_INTERVAL = 0.5  # Интервал проверки SL/TP для позиций в HOLD
POSITION_VALIDATION_INTERVAL = 30.0  # FIX Problem 1: Интервал сверки позиций с биржами (сек)


# ==============================
# FIX Problem 7: Background DB helpers
# ==============================

async def _save_entry_background(
    db: "DBManager",
    pair_id: int,
    long_exchange: str,
    short_exchange: str,
    filled_parts: int,
    closed_parts: int,
    entry_prices_long: list,
    entry_prices_short: list,
    part_volume: float,
    symbol: str,
    volume: float,
    spread_pct: float,
):
    """
    Асинхронное сохранение входа в DB в background.
    Не блокирует критический торговый путь.
    """
    try:
        await db.save_position(
            pair_id=pair_id,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            filled_parts=filled_parts,
            closed_parts=closed_parts,
            entry_prices_long=entry_prices_long,
            entry_prices_short=entry_prices_short,
            part_volume=part_volume,
        )
        await db.log_trade_event(
            pair_id, "ENTRY_OK", "info",
            f"Entry {symbol}: {long_exchange}->{short_exchange}, part 1",
            {"buy_exchange": long_exchange, "sell_exchange": short_exchange,
             "volume": volume, "spread_pct": spread_pct}
        )
    except Exception as e:
        logger.error(f"[{pair_id}] Background DB save failed: {e}")


# ==============================
# Класс состояния пары в памяти
# ==============================

@dataclass
class PairState:
    pair_id: int
    symbol: str
    total_volume: float
    n_orders: int
    entry_spread: float
    exit_spread: float
    stop_loss: float
    leverage: int = 10

    part_volume: float = field(init=False)
    filled_parts: int = 0
    closed_parts: int = 0
    long_exchange: Optional[str] = None
    short_exchange: Optional[str] = None
    entry_prices_long: List[float] = field(default_factory=list)
    entry_prices_short: List[float] = field(default_factory=list)
    exit_prices_long: List[float] = field(default_factory=list)
    exit_prices_short: List[float] = field(default_factory=list)
    status: str = STATE_READY
    actual_long_volume: float = 0.0
    actual_short_volume: float = 0.0

    # Event-driven: время последней обработки
    last_process_time: float = 0.0
    # FIX: asyncio.Lock вместо bool для атомарной защиты от race condition
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # FIX #1: Счётчик пропущенных проверок при stale данных
    missed_hold_checks: int = 0
    # FIX #6, #7: Таймауты для ENTERING и EXITING состояний
    entering_started_at: float = 0.0
    exiting_started_at: float = 0.0

    def __post_init__(self):
        self.n_orders = max(1, int(self.n_orders))
        self.part_volume = self.total_volume / self.n_orders if self.n_orders > 0 else 0.0

    @property
    def is_flat(self) -> bool:
        return self.filled_parts == 0 and self.closed_parts == 0

    @property
    def is_fully_entered(self) -> bool:
        return self.filled_parts >= self.n_orders

    @property
    def open_parts(self) -> int:
        return max(0, self.filled_parts - self.closed_parts)

    @property
    def open_volume(self) -> float:
        return self.open_parts * self.part_volume

    @property
    def volume_imbalance(self) -> float:
        return self.actual_long_volume - self.actual_short_volume

    def reset_after_exit(self):
        self.filled_parts = 0
        self.closed_parts = 0
        self.long_exchange = None
        self.short_exchange = None
        self.entry_prices_long.clear()
        self.entry_prices_short.clear()
        self.exit_prices_long.clear()
        self.exit_prices_short.clear()
        self.actual_long_volume = 0.0
        self.actual_short_volume = 0.0
        self.status = STATE_READY
        # FIX: Сброс новых полей
        self.missed_hold_checks = 0
        self.entering_started_at = 0.0
        self.exiting_started_at = 0.0


# ==============================
# Глобальный контроллер риска
# ==============================

# FIX Problem 3: TTL для кэша настроек (секунды)
# Оптимизировано: 10s → 60s (настройки меняются редко, сброс кэша при изменении через веб)
SETTINGS_CACHE_TTL = 60.0


class RiskController:
    """
    FIX Problem 3: Исправлен deadlock - DB запросы теперь вне lock.
    Используется кэширование max_open с TTL для избежания блокировки.
    """

    def __init__(self, db: DBManager):
        self.db = db
        self._lock = asyncio.Lock()
        self._open_pairs_count: int = 0
        self._current_risk_usdt: float = 0.0

        # FIX Problem 3: Кэш для max_open_positions
        self._cached_max_open: int = MAX_OPEN_PAIRS
        self._cache_updated_at: float = 0.0

    async def _get_max_open_cached(self) -> int:
        """
        FIX Problem 3: Получить max_open с кэшированием.
        Кэш обновляется раз в SETTINGS_CACHE_TTL секунд.
        Этот метод вызывается ВНЕ lock для предотвращения deadlock.
        """
        now = time.time()
        if now - self._cache_updated_at > SETTINGS_CACHE_TTL:
            try:
                self._cached_max_open = await asyncio.wait_for(
                    self.db.get_setting_int("max_open_positions", MAX_OPEN_PAIRS),
                    timeout=5.0  # 5 секунд таймаут на DB запрос
                )
                self._cache_updated_at = now
            except asyncio.TimeoutError:
                logger.warning("[RiskController] DB timeout getting max_open, using cached value")
            except Exception as e:
                logger.warning(f"[RiskController] DB error getting max_open: {e}, using cached value")
        return self._cached_max_open

    async def refresh_from_state(self, pair_states: Dict[int, PairState]):
        # FIX Problem 3: DB запросы вне lock
        try:
            risk_usdt = float(await asyncio.wait_for(
                self.db.get_total_open_notional(),
                timeout=5.0
            ))
        except (AttributeError, TypeError, asyncio.TimeoutError):
            risk_usdt = 0.0
        except Exception:
            risk_usdt = 0.0

        async with self._lock:
            self._open_pairs_count = sum(1 for s in pair_states.values() if s.open_parts > 0)
            self._current_risk_usdt = risk_usdt

    async def try_acquire_entry_slot(self, planned_notional: float) -> Tuple[bool, str]:
        # FIX Problem 3: Получаем max_open ВНЕ lock (может занять время из-за DB)
        max_open = await self._get_max_open_cached()

        # Теперь быстрая операция под lock (только in-memory)
        async with self._lock:
            remaining_slots = max_open - self._open_pairs_count
            if remaining_slots <= 0:
                return False, "NO_ENTRY_SLOTS"
            if MAX_PAIR_VOLUME is not None and MAX_PAIR_VOLUME > 0:
                if planned_notional > MAX_PAIR_VOLUME:
                    return False, "PAIR_VOLUME_EXCEEDS_MAX"
            if MAX_TOTAL_RISK_USDT is not None and MAX_TOTAL_RISK_USDT > 0:
                if self._current_risk_usdt + planned_notional > MAX_TOTAL_RISK_USDT:
                    return False, "TOTAL_RISK_LIMIT_EXCEEDED"
            self._open_pairs_count += 1
            self._current_risk_usdt += planned_notional
            return True, "OK"

    async def release_entry_slot(self, planned_notional: float):
        async with self._lock:
            self._open_pairs_count = max(0, self._open_pairs_count - 1)
            self._current_risk_usdt = max(0.0, self._current_risk_usdt - planned_notional)

    async def get_snapshot(self) -> Dict[str, Any]:
        # FIX Problem 3: Используем кэшированное значение
        max_open = await self._get_max_open_cached()
        return {
            "open_pairs_count": self._open_pairs_count,
            "max_open_positions": max_open,
            "remaining_slots": max(0, max_open - self._open_pairs_count),
            "current_risk_usdt": self._current_risk_usdt,
            "max_risk_usdt": MAX_TOTAL_RISK_USDT,
        }

    def invalidate_cache(self):
        """Сброс кэша настроек — вызывается при изменении через веб-интерфейс."""
        self._cache_updated_at = 0.0


def estimate_planned_position_notional(state: PairState, signal: dict) -> float:
    total_volume = state.total_volume
    buy_price = float(signal.get("buy_price", 0.0))
    sell_price = float(signal.get("sell_price", 0.0))
    avg_price = (buy_price + sell_price) / 2.0 if buy_price and sell_price else max(buy_price, sell_price, 0.0)
    return total_volume * avg_price


# ==============================
# Shutdown Manager
# ==============================

class ShutdownManager:
    def __init__(self):
        self._shutdown_requested = False
        self._shutdown_event = asyncio.Event()

    @property
    def is_shutdown_requested(self) -> bool:
        return self._shutdown_requested

    def request_shutdown(self):
        if not self._shutdown_requested:
            self._shutdown_requested = True
            self._shutdown_event.set()
            logger.warning("Shutdown requested!")

    async def wait_for_shutdown(self):
        await self._shutdown_event.wait()

    def setup_signal_handlers(self, loop: asyncio.AbstractEventLoop):
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_shutdown)
            except NotImplementedError:
                pass


async def graceful_close_all_positions(pair_states: Dict[int, PairState], trader: TradeEngine, db: DBManager):
    open_positions = [s for s in pair_states.values() if s.open_parts > 0 and s.long_exchange and s.short_exchange]
    if not open_positions:
        logger.info("No open positions to close at shutdown")
        return
    logger.warning(f"GRACEFUL SHUTDOWN: closing {len(open_positions)} positions...")
    tasks = [_emergency_close_position(s, trader, db) for s in open_positions]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    success = sum(1 for r in results if r is True)
    logger.info(f"Shutdown close results: {success} success, {len(results) - success} failed")


async def _emergency_close_position(state: PairState, trader: TradeEngine, db: DBManager) -> bool:
    try:
        position_info = {
            "symbol": state.symbol,
            "long_exchange": state.long_exchange,
            "short_exchange": state.short_exchange,
            "pair_id": state.pair_id,
            # FIX Problem 5: передаём actual volumes для корректного закрытия при дисбалансе
            "actual_long_volume": state.actual_long_volume,
            "actual_short_volume": state.actual_short_volume,
        }
        res = await trader.execute_exit(position_info, state.open_volume)
        if res["success"]:
            logger.info(f"[{state.pair_id}] Position closed at shutdown")
            await db.log_trade_event(state.pair_id, "EMERGENCY_CLOSE_OK", "warning", "Position closed at graceful shutdown", {"symbol": state.symbol})
            return True
        else:
            logger.error(f"[{state.pair_id}] Failed to close position at shutdown: {res.get('error')}")
            await db.log_trade_event(state.pair_id, "EMERGENCY_CLOSE_FAILED", "error", f"Error closing at shutdown: {res.get('error')}", {"symbol": state.symbol})
            return False
    except Exception as e:
        logger.exception(f"[{state.pair_id}] Exception at emergency close: {e}")
        return False


# ==============================
# EVENT-DRIVEN TRADING CORE
# ==============================

class TradingCore:
    """
    Event-driven trading core.
    Reacts to orderbook updates via callbacks instead of polling.
    """

    def __init__(self):
        self.db = DBManager()
        self.active_exchanges = []  # Will be set in async init()

        # These will be initialized in init() after we know exchanges
        self.ws = None
        self.ex_manager = ExchangeManager()
        self.market = None
        self.trader = None
        self.risk_controller = RiskController(self.db)
        self.shutdown_mgr = ShutdownManager()

        # State
        self.pair_states: Dict[int, PairState] = {}
        self.symbol_to_pairs: Dict[str, Set[int]] = {}  # symbol -> set of pair_ids
        self.subscribed_symbols: Set[str] = set()

        # Event processing
        self._process_lock = asyncio.Lock()
        self._symbol_last_process: Dict[str, float] = {}

        # Circuit breaker: disable exchange after N consecutive errors
        self._exchange_errors: Dict[str, int] = {}  # exchange -> error count
        self._exchange_disabled_until: Dict[str, float] = {}  # exchange -> timestamp
        self._circuit_breaker_threshold = 3  # errors before disable
        self._circuit_breaker_cooldown = 60.0  # seconds to disable

        # FIX #10: Heartbeat monitoring для loop'ов
        self._loop_heartbeats: Dict[str, float] = {
            "pair_sync": time.time(),
            "position_monitor": time.time(),
            "position_validation": time.time(),
            "risk_refresh": time.time(),
        }

        self._initialized = False

    async def init(self):
        """Async initialization - must be called before start()."""
        if self._initialized:
            return

        # Get connected exchanges (with API keys) from DB
        connected = await self.db.get_connected_exchanges()
        if connected:
            logger.info(f"Connected exchanges from DB: {connected}")
            self.active_exchanges = connected
        else:
            logger.warning("No connected exchanges in DB, using all from config")
            self.active_exchanges = EXCHANGES

        # Initialize WsManager with only connected exchanges
        self.ws = WsManager(allowed_exchanges=self.active_exchanges)
        self.market = MarketEngine(self.ws)
        self.trader = TradeEngine(self.ex_manager, self.db)

        self._initialized = True
        logger.info("TradingCore initialized")

    def circuit_breaker_record_error(self, exchange: str):
        """Record an error for circuit breaker."""
        ex = exchange.lower()
        self._exchange_errors[ex] = self._exchange_errors.get(ex, 0) + 1
        if self._exchange_errors[ex] >= self._circuit_breaker_threshold:
            self._exchange_disabled_until[ex] = time.time() + self._circuit_breaker_cooldown
            logger.error(f"CIRCUIT BREAKER: {ex} disabled for {self._circuit_breaker_cooldown}s after {self._exchange_errors[ex]} errors")

    def circuit_breaker_record_success(self, exchange: str):
        """Reset error count on success."""
        ex = exchange.lower()
        self._exchange_errors[ex] = 0

    def circuit_breaker_is_open(self, exchange: str) -> bool:
        """Check if exchange is disabled by circuit breaker."""
        ex = exchange.lower()
        disabled_until = self._exchange_disabled_until.get(ex, 0)
        if time.time() < disabled_until:
            return True  # Circuit is OPEN (blocked)
        # Reset if cooldown passed
        if disabled_until > 0:
            self._exchange_errors[ex] = 0
            self._exchange_disabled_until[ex] = 0
            logger.info(f"CIRCUIT BREAKER: {ex} re-enabled")
        return False  # Circuit is CLOSED (allowed)
        
    async def start(self):
        # Ensure async init is done
        if not self._initialized:
            await self.init()

        logger.info("TRADING CORE v4.0 STARTED (EVENT-DRIVEN, PURE ASYNC)")

        loop = asyncio.get_running_loop()
        self.shutdown_mgr.setup_signal_handlers(loop)

        # Start WebSocket manager
        await self.ws.start()
        logger.info("WebSocket manager started")
        
        # Register callback for orderbook updates
        self.ws.on_orderbook_update(self._on_orderbook_update)
        logger.info("Orderbook callback registered")

        # FIX #4: Register callback for liquidation events
        self.ws.on_liquidation(self._on_liquidation_event)
        logger.info("Liquidation callback registered")

        # Restore positions from DB
        await self._restore_positions()

        # Validate positions against exchanges (critical for crash recovery)
        await self._validate_positions()

        # Start background tasks
        asyncio.create_task(self._pair_sync_loop())
        asyncio.create_task(self._position_monitor_loop())
        asyncio.create_task(self._risk_refresh_loop())
        asyncio.create_task(self._position_validation_loop())  # FIX Problem 1
        asyncio.create_task(self._watchdog_loop())  # FIX #10: Watchdog

        # Wait for shutdown
        await self.shutdown_mgr.wait_for_shutdown()
        
    async def stop(self):
        logger.info("Stopping trading core...")
        await graceful_close_all_positions(self.pair_states, self.trader, self.db)
        await self.ws.stop()
        await self.ex_manager.close_all()
        logger.info("Trading core stopped")

    async def force_close_position(self, pair_id: int) -> dict:
        """
        Force close position by user request (button "Close Position").
        Closes both legs at market, pauses the pair.
        Returns: {"success": bool, "error": str|None, "pnl": float|None}
        """
        state = self.pair_states.get(pair_id)
        if not state:
            return {"success": False, "error": "pair_not_found", "pnl": None}

        if state.open_parts <= 0:
            return {"success": False, "error": "no_open_position", "pnl": None}

        if not state.long_exchange or not state.short_exchange:
            return {"success": False, "error": "missing_exchange_info", "pnl": None}

        symbol = state.symbol
        logger.warning(f"[{pair_id}] FORCE CLOSE requested by user | {symbol}")

        try:
            position_info = {
                "symbol": symbol,
                "long_exchange": state.long_exchange,
                "short_exchange": state.short_exchange,
                "pair_id": pair_id,
                # FIX Problem 5: передаём actual volumes для корректного закрытия при дисбалансе
                "actual_long_volume": state.actual_long_volume,
                "actual_short_volume": state.actual_short_volume,
            }

            res = await self.trader.execute_exit(position_info, state.open_volume)

            if res["success"]:
                # Calculate PnL
                exit_long = res.get("exit_long_price", 0)
                exit_short = res.get("exit_short_price", 0)

                avg_entry_long = sum(state.entry_prices_long) / len(state.entry_prices_long) if state.entry_prices_long else 0
                avg_entry_short = sum(state.entry_prices_short) / len(state.entry_prices_short) if state.entry_prices_short else 0

                volume = state.open_volume
                pnl_long = (exit_long - avg_entry_long) * volume if avg_entry_long > 0 else 0
                pnl_short = (avg_entry_short - exit_short) * volume if avg_entry_short > 0 else 0
                total_pnl = pnl_long + pnl_short

                # Update DB
                await self.db.update_pair_pnl(pair_id, total_pnl)
                await self.db.delete_position(pair_id)
                await self.db.update_pair_status(pair_id, "paused")
                await self.db.log_trade_event(pair_id, "FORCE_CLOSE", "warning",
                    f"Position force-closed by user | PnL={total_pnl:.2f}$",
                    {"symbol": symbol, "pnl": total_pnl})

                # Reset state
                state.reset_after_exit()
                state.status = STATE_PAUSED

                logger.info(f"[{pair_id}] FORCE CLOSE OK | PnL={total_pnl:.2f}$")
                return {"success": True, "error": None, "pnl": total_pnl}
            else:
                error = res.get("error", "unknown_error")
                await self.db.log_trade_event(pair_id, "FORCE_CLOSE_FAILED", "error",
                    f"Force close failed: {error}", {"symbol": symbol})
                logger.error(f"[{pair_id}] FORCE CLOSE FAILED | {error}")
                return {"success": False, "error": error, "pnl": None}

        except Exception as e:
            logger.exception(f"[{pair_id}] Exception in force_close_position: {e}")
            return {"success": False, "error": str(e), "pnl": None}

    def get_pair_state(self, pair_id: int) -> dict | None:
        """
        Get current state of a pair for web display.
        Returns None if pair not found.
        """
        state = self.pair_states.get(pair_id)
        if not state:
            return None

        return {
            "pair_id": pair_id,
            "symbol": state.symbol,
            "status": state.status,
            "has_position": state.open_parts > 0,
            "long_exchange": state.long_exchange,
            "short_exchange": state.short_exchange,
            "filled_parts": state.filled_parts,
            "closed_parts": state.closed_parts,
            "open_parts": state.open_parts,
            "total_volume": state.total_volume,
            "open_volume": state.open_volume,
            "part_volume": state.part_volume,
            "actual_long_volume": state.actual_long_volume,
            "actual_short_volume": state.actual_short_volume,
            "volume_imbalance": state.volume_imbalance,
            "entry_spread": state.entry_spread,
            "exit_spread": state.exit_spread,
            "stop_loss": state.stop_loss,
            "n_orders": state.n_orders,
            "leverage": state.leverage,
            "entry_prices_long": state.entry_prices_long.copy(),
            "entry_prices_short": state.entry_prices_short.copy(),
            "avg_entry_long": sum(state.entry_prices_long) / len(state.entry_prices_long) if state.entry_prices_long else 0,
            "avg_entry_short": sum(state.entry_prices_short) / len(state.entry_prices_short) if state.entry_prices_short else 0,
        }

    def get_all_pairs_state(self) -> list:
        """Get state of all monitored pairs for web display."""
        return [self.get_pair_state(pid) for pid in self.pair_states.keys()]

    def can_remove_exchange(self, exchange: str) -> dict:
        """
        Check if exchange can be safely removed.
        Returns: {"can_remove": bool, "reason": str|None, "blocking_pairs": list}
        """
        ex = exchange.lower()
        blocking_pairs = []

        for state in self.pair_states.values():
            if state.filled_parts > 0:
                if state.long_exchange == ex or state.short_exchange == ex:
                    blocking_pairs.append({
                        "pair_id": state.pair_id,
                        "symbol": state.symbol,
                        "side": "long" if state.long_exchange == ex else "short",
                    })

        if blocking_pairs:
            return {
                "can_remove": False,
                "reason": f"Active positions on {len(blocking_pairs)} pair(s)",
                "blocking_pairs": blocking_pairs,
            }

        return {"can_remove": True, "reason": None, "blocking_pairs": []}

    # ============================================================
    # SETTINGS API
    # ============================================================

    async def get_settings(self) -> dict:
        """Get all bot settings for web interface."""
        # Add current runtime values
        return {
            "max_open_positions": {
                "value": await self.db.get_setting_int("max_open_positions", MAX_OPEN_PAIRS),
                "description": "Максимум одновременных арбитражей",
                "min": 1,
                "max": 20,
            },
            "default_leverage": {
                "value": await self.db.get_setting_int("default_leverage", 10),
                "description": "Плечо по умолчанию для всех бирж",
                "min": 1,
                "max": 100,
            },
            "max_monitored_pairs": {
                "value": await self.db.get_setting_int("max_monitored_pairs", MAX_MONITORED_PAIRS),
                "description": "Максимум отслеживаемых пар",
                "min": 1,
                "max": 50,
            },
        }

    async def update_settings(self, new_settings: dict) -> dict:
        """
        Update bot settings.
        new_settings: {"max_open_positions": 5, "default_leverage": 10, ...}
        Returns: {"success": bool, "updated": list, "errors": list}
        """
        allowed_keys = {"max_open_positions", "default_leverage", "max_monitored_pairs"}
        updated = []
        errors = []

        for key, value in new_settings.items():
            if key not in allowed_keys:
                errors.append(f"Unknown setting: {key}")
                continue

            try:
                int_value = int(value)

                # Validation
                if key == "max_open_positions" and not (1 <= int_value <= 20):
                    errors.append(f"{key}: must be 1-20")
                    continue
                if key == "default_leverage" and not (1 <= int_value <= 100):
                    errors.append(f"{key}: must be 1-100")
                    continue
                if key == "max_monitored_pairs" and not (1 <= int_value <= 50):
                    errors.append(f"{key}: must be 1-50")
                    continue

                await self.db.set_setting(key, str(int_value))
                updated.append(key)
                logger.info(f"[SETTINGS] {key} = {int_value}")

            except (ValueError, TypeError) as e:
                errors.append(f"{key}: invalid value ({e})")

        # Сброс кэша настроек — новые значения применятся мгновенно
        if updated:
            self.risk_controller.invalidate_cache()

        return {"success": len(errors) == 0, "updated": updated, "errors": errors}

    async def get_max_open_positions(self) -> int:
        """Get current max open positions setting."""
        return await self.db.get_setting_int("max_open_positions", MAX_OPEN_PAIRS)

    async def get_default_leverage(self) -> int:
        """Get current default leverage setting."""
        return await self.db.get_setting_int("default_leverage", 10)

    async def delete_pair(self, pair_id: int, force: bool = False) -> dict:
        """
        Delete pair completely (button "Delete").
        If pair has open position and force=False, returns error.
        If force=True, closes position first then deletes.
        Returns: {"success": bool, "error": str|None}
        """
        state = self.pair_states.get(pair_id)

        # Check for open position
        if state and state.open_parts > 0:
            if not force:
                return {
                    "success": False,
                    "error": "has_open_position",
                    "message": "Pair has open position. Close it first or use force=True"
                }
            # Force close position first
            close_result = await self.force_close_position(pair_id)
            if not close_result["success"]:
                return {
                    "success": False,
                    "error": "force_close_failed",
                    "message": f"Failed to close position: {close_result.get('error')}"
                }

        # Remove from memory
        if state:
            symbol = state.symbol
            if pair_id in self.pair_states:
                del self.pair_states[pair_id]
            if symbol in self.symbol_to_pairs:
                self.symbol_to_pairs[symbol].discard(pair_id)

        # Delete from DB
        try:
            await self.db.delete_pair(pair_id)
            await self.db.log_trade_event(pair_id, "PAIR_DELETED", "info",
                f"Pair {pair_id} deleted by user", {})
            logger.info(f"[{pair_id}] Pair deleted by user")
            return {"success": True, "error": None}
        except Exception as e:
            logger.error(f"[{pair_id}] Failed to delete pair: {e}")
            return {"success": False, "error": str(e)}

    async def _on_orderbook_update(self, exchange: str, symbol: str, book: dict):
        """
        Callback from WsManager on every orderbook update.
        This is the heart of event-driven architecture.
        """
        # Debounce: skip if processed too recently
        now = time.time()
        last = self._symbol_last_process.get(symbol, 0)
        if (now - last) * 1000 < EVENT_DEBOUNCE_MS:
            return
        
        # Invalidate market engine cache for this symbol
        self.market.invalidate_symbol(symbol)
        
        # Get pairs watching this symbol
        pair_ids = self.symbol_to_pairs.get(symbol)
        if not pair_ids:
            return
        
        self._symbol_last_process[symbol] = now
        
        # Process each pair that watches this symbol
        for pair_id in list(pair_ids):
            state = self.pair_states.get(pair_id)
            if not state:
                continue

            # FIX: Используем Lock вместо bool
            if state._lock.locked():
                continue

            # Process READY states on market updates (entry opportunities)
            if state.status == STATE_READY:
                asyncio.create_task(self._process_ready_state(state))
            # FIX #11: Event-driven HOLD - мгновенная реакция на изменение спреда для TP
            elif state.status == STATE_HOLD:
                asyncio.create_task(self._process_hold_state_event(state))

    # FIX #4: Callback для мгновенной реакции на ликвидации
    async def _on_liquidation_event(self, exchange: str, symbol: str, liq_data: dict):
        """
        Callback from WsManager on liquidation events.
        Immediately closes the remaining leg when one leg is liquidated.
        """
        pair_ids = self.symbol_to_pairs.get(symbol)
        if not pair_ids:
            return

        for pair_id in list(pair_ids):
            state = self.pair_states.get(pair_id)
            if not state or state.open_parts <= 0:
                continue

            # Определяем какая нога была ликвидирована
            liq_side = liq_data.get("side", "").lower()

            # sell liquidation = LONG позиция ликвидирована (биржа продала нашу LONG)
            if liq_side == "sell" and state.long_exchange == exchange:
                logger.critical(
                    f"[{pair_id}] LIQUIDATION EVENT: LONG on {exchange} | "
                    f"Closing remaining SHORT on {state.short_exchange}"
                )
                await self.db.log_trade_event(
                    pair_id, "LIQUIDATION_CALLBACK", "critical",
                    f"LONG liquidated on {exchange}, closing SHORT",
                    {"exchange": exchange, "symbol": symbol, "liq_data": liq_data}
                )
                try:
                    close_result = await self.trader._emergency_close_leg(
                        exchange=state.short_exchange,
                        symbol=state.symbol,
                        side="buy",
                        amount=state.actual_short_volume,
                        leg_label="liquidation_callback_close_short",
                        pair_id=pair_id,
                    )
                    logger.info(f"[{pair_id}] Liquidation callback close SHORT result: {close_result}")
                except Exception as e:
                    logger.error(f"[{pair_id}] Failed to close SHORT after liquidation: {e}")
                state.reset_after_exit()
                state.status = STATE_ERROR
                await self.db.update_pair_status(pair_id, "error")

            # buy liquidation = SHORT позиция ликвидирована (биржа купила нашу SHORT)
            elif liq_side == "buy" and state.short_exchange == exchange:
                logger.critical(
                    f"[{pair_id}] LIQUIDATION EVENT: SHORT on {exchange} | "
                    f"Closing remaining LONG on {state.long_exchange}"
                )
                await self.db.log_trade_event(
                    pair_id, "LIQUIDATION_CALLBACK", "critical",
                    f"SHORT liquidated on {exchange}, closing LONG",
                    {"exchange": exchange, "symbol": symbol, "liq_data": liq_data}
                )
                try:
                    close_result = await self.trader._emergency_close_leg(
                        exchange=state.long_exchange,
                        symbol=state.symbol,
                        side="sell",
                        amount=state.actual_long_volume,
                        leg_label="liquidation_callback_close_long",
                        pair_id=pair_id,
                    )
                    logger.info(f"[{pair_id}] Liquidation callback close LONG result: {close_result}")
                except Exception as e:
                    logger.error(f"[{pair_id}] Failed to close LONG after liquidation: {e}")
                state.reset_after_exit()
                state.status = STATE_ERROR
                await self.db.update_pair_status(pair_id, "error")

    async def _process_ready_state(self, state: PairState):
        """Process a READY state pair when market data updates."""
        # FIX: Используем asyncio.Lock для атомарной проверки (предотвращает race condition)
        if state._lock.locked():
            return  # Уже обрабатывается другим callback'ом
        async with state._lock:
            try:
                await handle_state_ready(
                    self.db, self.market, self.trader,
                    None, state, self.risk_controller, self
                )
            except Exception as e:
                logger.error(f"[{state.pair_id}] Error in ready state: {e}")
            finally:
                state.last_process_time = time.time()

    # FIX #11: Event-driven обработка HOLD состояния
    async def _process_hold_state_event(self, state: PairState):
        """Process a HOLD state pair when market data updates (for faster TP detection)."""
        if state._lock.locked():
            return
        async with state._lock:
            try:
                await handle_state_hold(
                    self.db, self.market, self.trader,
                    None, state, core=self
                )
            except Exception as e:
                logger.error(f"[{state.pair_id}] Error in hold state event: {e}")
            finally:
                state.last_process_time = time.time()

    async def _restore_positions(self):
        """Restore open positions from database on startup."""
        try:
            open_positions = await self.db.get_open_positions_for_restore()
        except AttributeError:
            open_positions = []
            logger.info("DBManager does not support get_open_positions_for_restore()")
        
        for pos in open_positions:
            try:
                state = PairState(
                    pair_id=pos["pair_id"],
                    symbol=pos["symbol"],
                    total_volume=float(pos["total_volume"]),
                    n_orders=int(pos["n_orders"]),
                    entry_spread=float(pos["entry_spread"]),
                    exit_spread=float(pos["exit_spread"]),
                    stop_loss=float(pos.get("stop_loss") or 0.0),
                    leverage=int(pos.get("leverage") or 10),
                )
                state.status = pos.get("status", STATE_HOLD)
                state.long_exchange = pos.get("long_exchange")
                state.short_exchange = pos.get("short_exchange")
                state.filled_parts = int(pos.get("filled_parts", 0))
                state.closed_parts = int(pos.get("closed_parts", 0))
                state.entry_prices_long = list(pos.get("entry_prices_long", []))
                state.entry_prices_short = list(pos.get("entry_prices_short", []))
                state.exit_prices_long = list(pos.get("exit_prices_long", []))
                state.exit_prices_short = list(pos.get("exit_prices_short", []))
                state.actual_long_volume = float(pos.get("actual_long_volume", 0))
                state.actual_short_volume = float(pos.get("actual_short_volume", 0))
                
                self.pair_states[state.pair_id] = state
                
                # Subscribe to symbol
                symbol = state.symbol
                if symbol not in self.symbol_to_pairs:
                    self.symbol_to_pairs[symbol] = set()
                self.symbol_to_pairs[symbol].add(state.pair_id)
                
                for ex in self.active_exchanges:
                    await self.ws.subscribe(ex, symbol)
                
                logger.info(f"Restored position {state.pair_id} ({symbol}): status={state.status}")
            except Exception as e:
                logger.error(f"Error restoring position {pos.get('pair_id')}: {e}")

    async def _validate_positions(self):
        """Validate DB positions against real exchange positions."""
        if not self.pair_states:
            logger.info("No positions to validate")
            return

        logger.info(f"Validating {len(self.pair_states)} positions against exchanges...")

        for pair_id, state in list(self.pair_states.items()):
            if state.open_parts <= 0:
                continue
            if not state.long_exchange or not state.short_exchange:
                continue

            try:
                # Get real positions from exchanges
                real_long = await self.ex_manager.get_position(
                    state.long_exchange, state.symbol
                )
                real_short = await self.ex_manager.get_position(
                    state.short_exchange, state.symbol
                )

                db_volume = state.open_volume
                real_long_size = abs(float(real_long.get("contracts", 0) if real_long else 0))
                real_short_size = abs(float(real_short.get("contracts", 0) if real_short else 0))

                # Check for mismatch
                long_mismatch = abs(real_long_size - db_volume) > db_volume * 0.05
                short_mismatch = abs(real_short_size - db_volume) > db_volume * 0.05

                if long_mismatch or short_mismatch:
                    logger.critical(
                        f"POSITION MISMATCH pair={pair_id} {state.symbol} | "
                        f"DB={db_volume:.4f}, LONG={real_long_size:.4f}, SHORT={real_short_size:.4f}"
                    )
                    await self.db.log_trade_event(
                        pair_id, "POSITION_MISMATCH", "error",
                        f"DB vs Exchange mismatch detected",
                        {"db_volume": db_volume, "real_long": real_long_size, "real_short": real_short_size}
                    )
                    # Pause the pair - needs manual intervention
                    state.status = STATE_PAUSED
                    await self.db.update_pair_status(pair_id, "paused")
                else:
                    logger.info(f"Position {pair_id} ({state.symbol}) validated OK")

            except Exception as e:
                logger.warning(f"Could not validate position {pair_id}: {e}")

    async def _pair_sync_loop(self):
        """Background task to sync active pairs and exchanges from database."""
        while not self.shutdown_mgr.is_shutdown_requested:
            try:
                # FIX #10: Heartbeat update
                self._loop_heartbeats["pair_sync"] = time.time()

                # ============================================================
                # SYNC EXCHANGES (hot-reload new exchanges)
                # ============================================================
                db_exchanges = set(await self.db.get_connected_exchanges())
                current_exchanges = set(self.active_exchanges)

                # Add new exchanges
                new_exchanges = db_exchanges - current_exchanges
                for ex in new_exchanges:
                    added = await self.ws.add_exchange(ex)
                    if added:
                        self.active_exchanges.append(ex)
                        # Subscribe new exchange to all existing symbols
                        for symbol in self.symbol_to_pairs.keys():
                            await self.ws.subscribe(ex, symbol)
                        logger.info(f"[HOT-RELOAD] Exchange {ex} added and subscribed")

                # Remove old exchanges (with protection for active positions)
                removed_exchanges = current_exchanges - db_exchanges
                for ex in removed_exchanges:
                    # Check if any position uses this exchange
                    has_active_position = False
                    for state in self.pair_states.values():
                        if state.filled_parts > 0:
                            if state.long_exchange == ex or state.short_exchange == ex:
                                has_active_position = True
                                logger.warning(
                                    f"[HOT-RELOAD] Cannot remove {ex} - active position on pair {state.pair_id} ({state.symbol})"
                                )
                                break

                    if has_active_position:
                        continue  # Skip removal, keep exchange active

                    await self.ws.remove_exchange(ex)
                    if ex in self.active_exchanges:
                        self.active_exchanges.remove(ex)
                    logger.info(f"[HOT-RELOAD] Exchange {ex} removed")

                # ============================================================
                # SYNC PAIRS
                # ============================================================
                pairs = await self.db.get_active_pairs()
                if not pairs:
                    await asyncio.sleep(PRICE_UPDATE_INTERVAL)
                    continue

                max_monitored = await self.db.get_setting_int("max_monitored_pairs", MAX_MONITORED_PAIRS)
                if len(pairs) > max_monitored:
                    pairs = pairs[:max_monitored]

                active_ids = {p["id"] for p in pairs}
                
                # Remove pairs that are no longer active
                # BUT keep pairs with open positions to continue monitoring
                for pid in list(self.pair_states.keys()):
                    if pid not in active_ids:
                        state = self.pair_states.get(pid)
                        if state and state.filled_parts > 0:
                            # Pair was paused but has open position - keep monitoring for exit
                            logger.debug(f"Pair {pid} paused but has open position - keeping for exit monitoring")
                            continue
                        state = self.pair_states.pop(pid, None)
                        if state:
                            symbol = state.symbol
                            if symbol in self.symbol_to_pairs:
                                self.symbol_to_pairs[symbol].discard(pid)
                        logger.info(f"Removed inactive pair {pid}")
                
                # Add new pairs
                for p in pairs:
                    pair_id = p["id"]
                    if pair_id in self.pair_states:
                        continue
                    
                    state = PairState(
                        pair_id=pair_id,
                        symbol=p["symbol"],
                        total_volume=float(p["volume"]),
                        n_orders=int(p["n_orders"]),
                        entry_spread=float(p["entry_spread"]),
                        exit_spread=float(p["exit_spread"]),
                        stop_loss=float(p["stop_loss"]) if p["stop_loss"] is not None else 0.0,
                        leverage=int(p.get("leverage") or 10),
                    )
                    self.pair_states[pair_id] = state
                    
                    symbol = state.symbol
                    if symbol not in self.symbol_to_pairs:
                        self.symbol_to_pairs[symbol] = set()
                    self.symbol_to_pairs[symbol].add(pair_id)
                    
                    # Subscribe to orderbook for all exchanges
                    for ex in self.active_exchanges:
                        await self.ws.subscribe(ex, symbol)
                    
                    logger.info(f"Added pair {pair_id} ({symbol})")
                
            except Exception as e:
                logger.error(f"Error in pair sync loop: {e}")
            
            await asyncio.sleep(PRICE_UPDATE_INTERVAL)

    async def _position_monitor_loop(self):
        """Background task to monitor open positions for TP/SL."""
        while not self.shutdown_mgr.is_shutdown_requested:
            try:
                # FIX #10: Heartbeat update
                self._loop_heartbeats["position_monitor"] = time.time()

                for state in list(self.pair_states.values()):
                    # FIX: Используем Lock вместо bool
                    if state._lock.locked():
                        continue

                    if state.status == STATE_ENTERING:
                        async with state._lock:
                            await handle_state_entering(self.db, self.market, self.trader, None, state)

                    elif state.status == STATE_HOLD:
                        async with state._lock:
                            await handle_state_hold(self.db, self.market, self.trader, None, state, core=self)

                    elif state.status == STATE_EXITING:
                        async with state._lock:
                            await handle_state_exiting(self.db, self.market, self.trader, None, state, core=self)

            except Exception as e:
                logger.error(f"Error in position monitor loop: {e}")

            await asyncio.sleep(POSITION_CHECK_INTERVAL)

    async def _risk_refresh_loop(self):
        """Background task to refresh risk limits."""
        while not self.shutdown_mgr.is_shutdown_requested:
            try:
                # FIX #10: Heartbeat update
                self._loop_heartbeats["risk_refresh"] = time.time()
                await self.risk_controller.refresh_from_state(self.pair_states)
            except Exception as e:
                logger.error(f"Error refreshing risk: {e}")
            await asyncio.sleep(1.0)

    # FIX Problem 1: Периодическая сверка позиций с биржами
    async def _position_validation_loop(self):
        """
        Background task to periodically validate positions against exchanges.

        Detects:
        - Liquidations (position closed by exchange)
        - Manual closes (position closed outside bot)
        - Volume mismatches (partial fills not tracked)
        """
        while not self.shutdown_mgr.is_shutdown_requested:
            try:
                await asyncio.sleep(POSITION_VALIDATION_INTERVAL)
                # FIX #10: Heartbeat update
                self._loop_heartbeats["position_validation"] = time.time()

                if not self.pair_states:
                    continue

                # Only validate pairs with open positions
                pairs_to_validate = [
                    (pid, state) for pid, state in self.pair_states.items()
                    if state.open_parts > 0 and state.long_exchange and state.short_exchange
                ]

                if not pairs_to_validate:
                    continue

                logger.debug(f"[VALIDATION] Checking {len(pairs_to_validate)} positions...")

                for pair_id, state in pairs_to_validate:
                    if self.shutdown_mgr.is_shutdown_requested:
                        break

                    try:
                        # Get real positions from exchanges
                        real_long = await self.ex_manager.get_position(
                            state.long_exchange, state.symbol
                        )
                        real_short = await self.ex_manager.get_position(
                            state.short_exchange, state.symbol
                        )

                        real_long_size = abs(float(real_long.get("contracts", 0) if real_long else 0))
                        real_short_size = abs(float(real_short.get("contracts", 0) if real_short else 0))

                        # Expected volumes from state
                        expected_long = state.actual_long_volume if state.actual_long_volume > 0 else state.open_volume
                        expected_short = state.actual_short_volume if state.actual_short_volume > 0 else state.open_volume

                        # Check for liquidation (both legs gone)
                        if real_long_size < expected_long * 0.05 and real_short_size < expected_short * 0.05:
                            logger.critical(
                                f"[VALIDATION] LIQUIDATION DETECTED pair={pair_id} {state.symbol} | "
                                f"Expected: L={expected_long:.4f} S={expected_short:.4f} | "
                                f"Real: L={real_long_size:.4f} S={real_short_size:.4f}"
                            )
                            await self.db.log_trade_event(
                                pair_id, "LIQUIDATION_DETECTED", "critical",
                                "Both legs appear liquidated or closed externally",
                                {"expected_long": expected_long, "expected_short": expected_short,
                                 "real_long": real_long_size, "real_short": real_short_size}
                            )
                            # Reset state and pause
                            state.reset_after_exit()
                            state.status = STATE_PAUSED
                            await self.db.update_pair_status(pair_id, "paused")
                            await self.db.delete_position(pair_id)
                            continue

                        # Check for single-leg liquidation (one leg gone)
                        long_missing = real_long_size < expected_long * 0.5
                        short_missing = real_short_size < expected_short * 0.5

                        # FIX #5: Auto-close оставшейся ноги при single-leg liquidation
                        if long_missing and not short_missing:
                            logger.critical(
                                f"[VALIDATION] LONG LEG MISSING pair={pair_id} {state.symbol} | "
                                f"Real LONG={real_long_size:.4f}, Expected={expected_long:.4f} | "
                                f"AUTO-CLOSING SHORT"
                            )
                            await self.db.log_trade_event(
                                pair_id, "LONG_LEG_MISSING", "critical",
                                "Long leg liquidated/closed - AUTO-CLOSING SHORT",
                                {"expected_long": expected_long, "real_long": real_long_size}
                            )
                            # Закрываем оставшуюся SHORT ногу
                            try:
                                close_result = await self.trader._emergency_close_leg(
                                    exchange=state.short_exchange,
                                    symbol=state.symbol,
                                    side="buy",  # buy to close short
                                    amount=real_short_size,
                                    leg_label="emergency_close_short_after_long_liquidation",
                                    pair_id=pair_id,
                                )
                                if close_result.get("success"):
                                    logger.info(f"[VALIDATION] SHORT leg closed successfully for pair={pair_id}")
                                else:
                                    logger.error(f"[VALIDATION] Failed to close SHORT: {close_result}")
                            except Exception as close_err:
                                logger.error(f"[VALIDATION] Exception closing SHORT: {close_err}")
                            state.reset_after_exit()
                            state.status = STATE_ERROR
                            await self.db.update_pair_status(pair_id, "error")
                            continue

                        if short_missing and not long_missing:
                            logger.critical(
                                f"[VALIDATION] SHORT LEG MISSING pair={pair_id} {state.symbol} | "
                                f"Real SHORT={real_short_size:.4f}, Expected={expected_short:.4f} | "
                                f"AUTO-CLOSING LONG"
                            )
                            await self.db.log_trade_event(
                                pair_id, "SHORT_LEG_MISSING", "critical",
                                "Short leg liquidated/closed - AUTO-CLOSING LONG",
                                {"expected_short": expected_short, "real_short": real_short_size}
                            )
                            # Закрываем оставшуюся LONG ногу
                            try:
                                close_result = await self.trader._emergency_close_leg(
                                    exchange=state.long_exchange,
                                    symbol=state.symbol,
                                    side="sell",  # sell to close long
                                    amount=real_long_size,
                                    leg_label="emergency_close_long_after_short_liquidation",
                                    pair_id=pair_id,
                                )
                                if close_result.get("success"):
                                    logger.info(f"[VALIDATION] LONG leg closed successfully for pair={pair_id}")
                                else:
                                    logger.error(f"[VALIDATION] Failed to close LONG: {close_result}")
                            except Exception as close_err:
                                logger.error(f"[VALIDATION] Exception closing LONG: {close_err}")
                            state.reset_after_exit()
                            state.status = STATE_ERROR
                            await self.db.update_pair_status(pair_id, "error")
                            continue

                        # Check for significant mismatch (>10% diff)
                        long_mismatch = abs(real_long_size - expected_long) > expected_long * 0.10
                        short_mismatch = abs(real_short_size - expected_short) > expected_short * 0.10

                        if long_mismatch or short_mismatch:
                            logger.warning(
                                f"[VALIDATION] VOLUME MISMATCH pair={pair_id} {state.symbol} | "
                                f"Expected: L={expected_long:.4f} S={expected_short:.4f} | "
                                f"Real: L={real_long_size:.4f} S={real_short_size:.4f}"
                            )
                            # Update actual volumes to match reality
                            state.actual_long_volume = real_long_size
                            state.actual_short_volume = real_short_size
                            await self.db.log_trade_event(
                                pair_id, "VOLUME_SYNC", "warning",
                                "Synced actual volumes with exchange",
                                {"new_long": real_long_size, "new_short": real_short_size}
                            )

                    except Exception as e:
                        logger.warning(f"[VALIDATION] Error checking pair {pair_id}: {e}")

            except Exception as e:
                logger.error(f"Error in position validation loop: {e}")

    # FIX #10: Watchdog loop для мониторинга heartbeat'ов
    async def _watchdog_loop(self):
        """Watchdog to detect frozen/dead loops."""
        WATCHDOG_TIMEOUT = 120  # 2 минуты без heartbeat = проблема
        while not self.shutdown_mgr.is_shutdown_requested:
            try:
                now = time.time()
                for loop_name, last_beat in self._loop_heartbeats.items():
                    age = now - last_beat
                    if age > WATCHDOG_TIMEOUT:
                        logger.critical(
                            f"⚠️ WATCHDOG: {loop_name} not responding for {age:.0f}s! "
                            f"Last heartbeat: {WATCHDOG_TIMEOUT}s ago"
                        )
                        await self.db.log_trade_event(
                            0, "WATCHDOG_ALERT", "critical",
                            f"Loop {loop_name} not responding",
                            {"loop": loop_name, "last_heartbeat_age": age}
                        )
            except Exception as e:
                logger.error(f"Error in watchdog loop: {e}")
            await asyncio.sleep(30)  # Проверка каждые 30 секунд


# ==============================
# State Handlers (optimized)
# ==============================

async def handle_state_ready(
    db: DBManager,
    market: MarketEngine,
    trader: TradeEngine,
    pair_row: dict,  # unused, kept for compatibility
    state: PairState,
    risk_controller: RiskController,
    core: "TradingCore" = None,
):
    """READY state: look for entry opportunity using O(N) fast path."""
    if not state.is_flat:
        return

    symbol = state.symbol
    monitor_volume = state.part_volume

    # Step 1: Fast screening (top of book) - O(N)
    try:
        signal = market.find_best_opportunity_fast(
            symbol=symbol,
            volume_in_coin=monitor_volume,
            min_spread_pct=state.entry_spread
        )
    except AttributeError:
        # Fallback to full search (uses active exchanges from WsManager)
        active_ex = market.ws.get_active_exchanges() if hasattr(market.ws, 'get_active_exchanges') else EXCHANGES
        signal = await market.find_best_opportunity(
            symbol=symbol,
            volume_in_coin=monitor_volume,
            exchanges=active_ex,
            min_spread_pct=state.entry_spread
        )

    if not signal:
        return

    buy_ex = signal["buy_exchange"]
    sell_ex = signal["sell_exchange"]

    # Step 2: Health check - are both exchanges alive?
    if not market.ws.is_healthy(buy_ex) or not market.ws.is_healthy(sell_ex):
        return  # Don't enter if WebSocket is dead/stale

    # Step 3: Circuit breaker - is exchange temporarily disabled?
    if core:
        if core.circuit_breaker_is_open(buy_ex) or core.circuit_breaker_is_open(sell_ex):
            return  # Exchange disabled after consecutive errors

    # Step 4: First VWAP check (preliminary)
    first_check_time = time.time()
    verified_signal = await market.check_spread(
        symbol=symbol,
        buy_exchange=buy_ex,
        sell_exchange=sell_ex,
        volume_in_coin=monitor_volume,
    )

    if not verified_signal:
        return  # Not enough liquidity in orderbook for our volume

    real_spread = verified_signal["net_full_spread_pct"]
    if real_spread < state.entry_spread:
        return  # After VWAP calculation, spread is below threshold

    # Step 5: Risk check
    planned_notional = estimate_planned_position_notional(state, verified_signal)
    allowed, reason = await risk_controller.try_acquire_entry_slot(planned_notional)

    if not allowed:
        return

    # Step 6: FINAL spread check (TOCTOU protection)
    # PERF: Если прошло < 50мс — используем первый результат (экономия 2-15мс)
    time_since_first_check = time.time() - first_check_time
    if time_since_first_check < 0.05:
        # Данные ещё свежие, не тратим время на повторный запрос
        final_signal = verified_signal
        final_spread = real_spread
    else:
        # Прошло > 50мс, перепроверяем спред
        final_signal = await market.check_spread(
            symbol=symbol,
            buy_exchange=buy_ex,
            sell_exchange=sell_ex,
            volume_in_coin=monitor_volume,
        )

        if not final_signal:
            await risk_controller.release_entry_slot(planned_notional)
            return

        final_spread = final_signal["net_full_spread_pct"]
        if final_spread < state.entry_spread:
            await risk_controller.release_entry_slot(planned_notional)
            logger.debug(f"[{state.pair_id}] ENTRY ABORTED: spread dropped {real_spread:.3f}% -> {final_spread:.3f}%")
            return

    logger.info(f"[{state.pair_id}] ENTRY {symbol} | {buy_ex}->{sell_ex} spread={final_spread}% (VWAP, verified)")

    res = await trader.execute_entry(final_signal, monitor_volume, pair_id=state.pair_id, leverage=state.leverage)

    # Circuit breaker: record success/error
    if res["success"]:
        if core:
            core.circuit_breaker_record_success(buy_ex)
            core.circuit_breaker_record_success(sell_ex)
        long_order = res.get("entry_long_order", {})
        short_order = res.get("entry_short_order", {})
        filled_long = float(long_order.get("filled") or monitor_volume)
        filled_short = float(short_order.get("filled") or monitor_volume)
        
        state.long_exchange = buy_ex
        state.short_exchange = sell_ex
        state.filled_parts = 1
        state.closed_parts = 0
        state.entry_prices_long = [signal["buy_price"]]
        state.entry_prices_short = [signal["sell_price"]]
        state.actual_long_volume = filled_long
        state.actual_short_volume = filled_short

        imbalance = abs(filled_long - filled_short)
        imbalance_pct = (imbalance / monitor_volume * 100) if monitor_volume > 0 else 0
        if imbalance_pct > 5:
            logger.warning(f"[{state.pair_id}] VOLUME IMBALANCE: {imbalance_pct:.2f}%")
            # Background log for imbalance warning
            asyncio.create_task(db.log_trade_event(state.pair_id, "VOLUME_IMBALANCE", "warning",
                f"Volume imbalance: {imbalance_pct:.2f}%",
                {"filled_long": filled_long, "filled_short": filled_short, "requested": monitor_volume}))

        # FIX Problem 7: DB операции в background - не блокируем критический путь
        # Сначала обновляем статус (мгновенно), потом сохраняем в DB асинхронно
        state.status = STATE_ENTERING if state.n_orders > 1 else STATE_HOLD

        asyncio.create_task(_save_entry_background(
            db=db,
            pair_id=state.pair_id,
            long_exchange=state.long_exchange,
            short_exchange=state.short_exchange,
            filled_parts=state.filled_parts,
            closed_parts=state.closed_parts,
            entry_prices_long=state.entry_prices_long.copy(),  # copy to avoid race
            entry_prices_short=state.entry_prices_short.copy(),
            part_volume=state.part_volume,
            symbol=symbol,
            volume=monitor_volume,
            spread_pct=real_spread,
        ))
    else:
        # Circuit breaker: record error
        if core:
            error_code = res.get("error") or ""
            # Determine which exchange failed
            if "long" in error_code.lower() or "second_leg" in error_code.lower():
                core.circuit_breaker_record_error(sell_ex)
            elif "short" in error_code.lower():
                core.circuit_breaker_record_error(buy_ex)
            else:
                # Unknown - record for both
                core.circuit_breaker_record_error(buy_ex)
                core.circuit_breaker_record_error(sell_ex)

        await risk_controller.release_entry_slot(planned_notional)
        error_code = res.get("error") or "ENTRY_ERROR"
        if error_code == "second_leg_failed_emergency_close":
            state.status = STATE_PAUSED
            # FIX Problem 7: DB в background
            asyncio.create_task(db.update_pair_status(state.pair_id, "paused"))
            asyncio.create_task(db.log_trade_event(state.pair_id, "SECOND_LEG_FAILED", "error",
                "Second leg failed, LONG closed emergency. Pair paused.", {"symbol": symbol}))
        else:
            # FIX Problem 7: DB в background
            asyncio.create_task(db.log_trade_event(state.pair_id, "ENTRY_ERROR", "error",
                f"Entry error: {error_code}", {"symbol": symbol}))


ENTERING_TIMEOUT = 300  # 5 минут таймаут для ENTERING состояния

async def handle_state_entering(
    db: DBManager,
    market: MarketEngine,
    trader: TradeEngine,
    pair_row: dict,
    state: PairState,
):
    """ENTERING state: add more parts while spread is good."""
    if state.is_fully_entered:
        state.entering_started_at = 0.0  # Сбросить таймер
        state.status = STATE_HOLD
        return

    if not state.long_exchange or not state.short_exchange:
        state.status = STATE_ERROR
        return

    # FIX #6: Таймаут ENTERING с Force Close
    if state.entering_started_at == 0.0:
        state.entering_started_at = time.time()

    if time.time() - state.entering_started_at > ENTERING_TIMEOUT:
        logger.critical(
            f"[{state.pair_id}] ENTERING TIMEOUT ({ENTERING_TIMEOUT}s) - FORCE CLOSING"
        )
        await db.log_trade_event(
            state.pair_id, "ENTERING_TIMEOUT", "critical",
            f"Entering timeout after {ENTERING_TIMEOUT}s, force closing",
            {"filled_parts": state.filled_parts, "n_orders": state.n_orders}
        )
        # Принудительное закрытие всех открытых позиций
        if state.filled_parts > 0 and state.actual_long_volume > 0:
            position_info = {
                "symbol": state.symbol,
                "long_exchange": state.long_exchange,
                "short_exchange": state.short_exchange,
                "pair_id": state.pair_id,
                "actual_long_volume": state.actual_long_volume,
                "actual_short_volume": state.actual_short_volume,
            }
            try:
                await trader.execute_exit(position_info, state.open_volume)
            except Exception as e:
                logger.error(f"[{state.pair_id}] Force close on ENTERING timeout failed: {e}")
        state.reset_after_exit()
        state.status = STATE_ERROR
        await db.update_pair_status(state.pair_id, "error")
        return

    symbol = state.symbol
    monitor_volume = state.part_volume

    signal = await market.check_spread(
        symbol=symbol,
        buy_exchange=state.long_exchange,
        sell_exchange=state.short_exchange,
        volume_in_coin=monitor_volume,
    )
    if not signal:
        return

    net_spread = signal["net_full_spread_pct"]

    if net_spread <= state.exit_spread and state.filled_parts > 0:
        logger.info(f"[{state.pair_id}] ENTERING->EXITING {symbol} | spread={net_spread}% <= exit={state.exit_spread}%")
        state.status = STATE_EXITING
        return

    if net_spread < state.entry_spread:
        return

    res = await trader.execute_entry(signal, monitor_volume, pair_id=state.pair_id, leverage=state.leverage)

    if res["success"]:
        long_order = res.get("entry_long_order", {})
        short_order = res.get("entry_short_order", {})
        filled_long = float(long_order.get("filled") or monitor_volume)
        filled_short = float(short_order.get("filled") or monitor_volume)
        
        state.filled_parts += 1
        state.entry_prices_long.append(signal["buy_price"])
        state.entry_prices_short.append(signal["sell_price"])
        state.actual_long_volume += filled_long
        state.actual_short_volume += filled_short

        # FIX 1.2: DB операции в background - не блокируем критический путь
        asyncio.create_task(db.save_position(
            pair_id=state.pair_id,
            long_exchange=state.long_exchange,
            short_exchange=state.short_exchange,
            filled_parts=state.filled_parts,
            closed_parts=state.closed_parts,
            entry_prices_long=state.entry_prices_long.copy(),  # copy to avoid race
            entry_prices_short=state.entry_prices_short.copy(),
            part_volume=state.part_volume,
        ))

        asyncio.create_task(db.log_trade_event(state.pair_id, "ENTRY_OK", "info",
            f"Additional entry {symbol}: part {state.filled_parts}/{state.n_orders}",
            {"volume": monitor_volume, "spread_pct": net_spread}))

        if state.is_fully_entered:
            state.status = STATE_HOLD
    else:
        error_code = res.get("error") or "ENTRY_ERROR"
        if error_code == "second_leg_failed_emergency_close":
            await db.update_pair_status(state.pair_id, "paused")
            await db.log_trade_event(state.pair_id, "SECOND_LEG_FAILED", "error", "Second leg failed during adding", {"symbol": symbol})
            state.status = STATE_PAUSED
        else:
            await db.log_trade_event(state.pair_id, "ENTRY_ERROR", "error", f"Error adding: {error_code}", {"symbol": symbol})
            state.status = STATE_HOLD


async def handle_state_hold(
    db: DBManager,
    market: MarketEngine,
    trader: TradeEngine,
    pair_row: dict,
    state: PairState,
    core: "TradingCore" = None,  # FIX #3: для проверки circuit breaker
):
    """HOLD state: monitor for TP/SL."""
    if state.open_parts <= 0:
        await db.delete_position(state.pair_id)
        state.status = STATE_READY
        return

    if not state.long_exchange or not state.short_exchange:
        state.status = STATE_ERROR
        return

    # FIX #3: Circuit Breaker check for HOLD state
    if core:
        if core.circuit_breaker_is_open(state.long_exchange) or core.circuit_breaker_is_open(state.short_exchange):
            logger.warning(f"[{state.pair_id}] HOLD: Circuit breaker open, skipping check")
            return

    symbol = state.symbol
    open_volume = state.open_volume

    avg_long_entry = sum(state.entry_prices_long) / len(state.entry_prices_long)
    avg_short_entry = sum(state.entry_prices_short) / len(state.entry_prices_short)

    pos_prices = await market.get_position_prices(
        symbol=symbol,
        long_exchange=state.long_exchange,
        short_exchange=state.short_exchange,
        volume_in_coin=open_volume,
    )

    # FIX #1: Счётчик пропущенных проверок при stale данных
    if not pos_prices or not pos_prices["valid"]:
        state.missed_hold_checks += 1
        if state.missed_hold_checks >= 20:  # 10 секунд при 0.5с интервале
            logger.critical(
                f"[{state.pair_id}] STALE DATA ESCALATION: {state.missed_hold_checks} missed checks | "
                f"FORCE CLOSING position"
            )
            await db.log_trade_event(
                state.pair_id, "STALE_DATA_ESCALATION", "critical",
                f"Force closing due to {state.missed_hold_checks} missed checks",
                {"missed_checks": state.missed_hold_checks}
            )
            # Принудительное закрытие позиции
            position_info = {
                "symbol": symbol,
                "long_exchange": state.long_exchange,
                "short_exchange": state.short_exchange,
                "pair_id": state.pair_id,
                "actual_long_volume": state.actual_long_volume,
                "actual_short_volume": state.actual_short_volume,
            }
            try:
                await trader.execute_exit(position_info, open_volume)
            except Exception as e:
                logger.error(f"[{state.pair_id}] Force close failed: {e}")
            state.reset_after_exit()
            state.status = STATE_ERROR
            await db.update_pair_status(state.pair_id, "error")
        return
    state.missed_hold_checks = 0  # Сбросить счётчик при успехе

    long_exit_price = pos_prices["long_exit_price"]
    short_exit_price = pos_prices["short_exit_price"]

    pnl_long = (long_exit_price - avg_long_entry) * open_volume
    pnl_short = (avg_short_entry - short_exit_price) * open_volume
    total_pnl = pnl_long + pnl_short

    is_sl = state.stop_loss > 0 and total_pnl <= -state.stop_loss

    signal = await market.check_spread(
        symbol=symbol,
        buy_exchange=state.long_exchange,
        sell_exchange=state.short_exchange,
        volume_in_coin=state.part_volume,
    )
    net_spread = signal["net_full_spread_pct"] if signal else None
    is_tp = (net_spread is not None) and (net_spread <= state.exit_spread)

    if is_sl:
        logger.warning(f"[{state.pair_id}] SL TRIGGERED {symbol} | PnL={total_pnl:.2f}$ <= -{state.stop_loss}$")
        # FIX Problem 5: передаём actual volumes для корректного закрытия при дисбалансе
        position_info = {
            "symbol": symbol,
            "long_exchange": state.long_exchange,
            "short_exchange": state.short_exchange,
            "pair_id": state.pair_id,
            "actual_long_volume": state.actual_long_volume,
            "actual_short_volume": state.actual_short_volume,
        }
        res = await trader.execute_exit(position_info, open_volume)

        if res["success"]:
            # FIX 1.3: DB операции в background - не блокируем после SL
            asyncio.create_task(db.update_pair_pnl(state.pair_id, total_pnl))
            asyncio.create_task(db.increment_sl(state.pair_id))
            asyncio.create_task(db.update_pair_status(state.pair_id, "paused"))
            asyncio.create_task(db.log_trade_event(state.pair_id, "SL_TRIGGERED", "error",
                f"SL on {symbol}: PnL={total_pnl:.2f}$, pair paused",
                {"pnl": total_pnl, "stop_loss": state.stop_loss}))
            asyncio.create_task(db.delete_position(state.pair_id))
            state.reset_after_exit()
            state.status = STATE_PAUSED
        else:
            await db.log_trade_event(state.pair_id, "EXIT_ERROR", "error", f"Failed to close by SL: {res.get('error')}", {"symbol": symbol})
        return

    if is_tp:
        logger.info(f"[{state.pair_id}] TP CONDITION {symbol} | spread={net_spread}% <= exit={state.exit_spread}%")
        state.status = STATE_EXITING


EXITING_TIMEOUT = 300  # 5 минут таймаут для EXITING состояния

async def handle_state_exiting(
    db: DBManager,
    market: MarketEngine,
    trader: TradeEngine,
    pair_row: dict,
    state: PairState,
    core: "TradingCore" = None,  # FIX #3: для проверки circuit breaker
):
    """EXITING state: close position in parts."""
    if state.open_parts <= 0:
        state.exiting_started_at = 0.0  # Сбросить таймер
        await finalize_full_exit(db, market, state)
        return

    if not state.long_exchange or not state.short_exchange:
        state.status = STATE_ERROR
        return

    # FIX #3: Circuit Breaker check for EXITING state
    if core:
        if core.circuit_breaker_is_open(state.long_exchange) or core.circuit_breaker_is_open(state.short_exchange):
            logger.warning(f"[{state.pair_id}] EXITING: Circuit breaker open, waiting for recovery")
            return

    # FIX #7: Таймаут EXITING с Force Close
    if state.exiting_started_at == 0.0:
        state.exiting_started_at = time.time()

    symbol = state.symbol
    volume_to_close = state.part_volume

    # Проверка таймаута - принудительное закрытие БЕЗ проверки спреда
    if time.time() - state.exiting_started_at > EXITING_TIMEOUT:
        logger.critical(
            f"[{state.pair_id}] EXITING TIMEOUT ({EXITING_TIMEOUT}s) - FORCE MARKET CLOSE"
        )
        await db.log_trade_event(
            state.pair_id, "EXITING_TIMEOUT", "critical",
            f"Exiting timeout after {EXITING_TIMEOUT}s, force market close",
            {"open_parts": state.open_parts, "volume": volume_to_close}
        )
        # Принудительное закрытие без проверки спреда
        position_info = {
            "symbol": symbol,
            "long_exchange": state.long_exchange,
            "short_exchange": state.short_exchange,
            "pair_id": state.pair_id,
            "actual_long_volume": state.actual_long_volume,
            "actual_short_volume": state.actual_short_volume,
        }
        try:
            res = await trader.execute_exit(position_info, state.open_volume)
            if res["success"]:
                state.exiting_started_at = 0.0
                await finalize_full_exit(db, market, state)
            else:
                logger.error(f"[{state.pair_id}] Force close failed: {res}")
                state.status = STATE_ERROR
                await db.update_pair_status(state.pair_id, "error")
        except Exception as e:
            logger.error(f"[{state.pair_id}] Force close exception: {e}")
            state.status = STATE_ERROR
            await db.update_pair_status(state.pair_id, "error")
        return

    signal = await market.check_spread(
        symbol=symbol,
        buy_exchange=state.long_exchange,
        sell_exchange=state.short_exchange,
        volume_in_coin=volume_to_close,
    )
    if not signal:
        return

    net_spread = signal["net_full_spread_pct"]

    if net_spread > state.exit_spread:
        return

    # FIX Problem 5: передаём actual volumes для корректного закрытия при дисбалансе
    # При частичном выходе пропорционально уменьшаем actual volumes
    parts_remaining = state.open_parts
    if parts_remaining > 0 and state.actual_long_volume > 0 and state.actual_short_volume > 0:
        # Закрываем пропорциональную часть от actual volumes
        actual_long_part = state.actual_long_volume / parts_remaining
        actual_short_part = state.actual_short_volume / parts_remaining
    else:
        actual_long_part = volume_to_close
        actual_short_part = volume_to_close

    position_info = {
        "symbol": symbol,
        "long_exchange": state.long_exchange,
        "short_exchange": state.short_exchange,
        "pair_id": state.pair_id,
        "actual_long_volume": actual_long_part,
        "actual_short_volume": actual_short_part,
    }
    res = await trader.execute_exit(position_info, volume_to_close)

    if res["success"]:
        state.closed_parts += 1
        # FIX Problem 5: уменьшаем actual volumes после частичного закрытия
        state.actual_long_volume = max(0.0, state.actual_long_volume - actual_long_part)
        state.actual_short_volume = max(0.0, state.actual_short_volume - actual_short_part)

        pos_prices = await market.get_position_prices(
            symbol=symbol,
            long_exchange=state.long_exchange,
            short_exchange=state.short_exchange,
            volume_in_coin=volume_to_close,
        )
        if pos_prices and pos_prices["valid"]:
            state.exit_prices_long.append(pos_prices["long_exit_price"])
            state.exit_prices_short.append(pos_prices["short_exit_price"])

        await db.save_position(
            pair_id=state.pair_id,
            long_exchange=state.long_exchange,
            short_exchange=state.short_exchange,
            filled_parts=state.filled_parts,
            closed_parts=state.closed_parts,
            entry_prices_long=state.entry_prices_long,
            entry_prices_short=state.entry_prices_short,
            part_volume=state.part_volume,
        )

        await db.log_trade_event(state.pair_id, "EXIT_PART_OK", "info",
            f"Partial exit {symbol}: part {state.closed_parts}/{state.filled_parts}",
            {"volume_closed": volume_to_close, "spread_pct": net_spread})

        if state.open_parts <= 0:
            await finalize_full_exit(db, market, state)
    else:
        await db.log_trade_event(state.pair_id, "EXIT_ERROR", "error", f"Partial exit error: {res.get('error')}", {"symbol": symbol})


async def finalize_full_exit(db: DBManager, market: MarketEngine, state: PairState):
    """Finalize full exit - calculate PnL."""
    if state.filled_parts <= 0:
        state.reset_after_exit()
        state.status = STATE_READY
        return

    total_volume = state.part_volume * state.filled_parts

    if state.exit_prices_long and state.exit_prices_short:
        avg_long_exit = sum(state.exit_prices_long) / len(state.exit_prices_long)
        avg_short_exit = sum(state.exit_prices_short) / len(state.exit_prices_short)
    else:
        logger.warning(f"[{state.pair_id}] finalize_full_exit: no exit prices, using market prices")
        pos_prices = await market.get_position_prices(
            symbol=state.symbol,
            long_exchange=state.long_exchange,
            short_exchange=state.short_exchange,
            volume_in_coin=total_volume,
        )
        if pos_prices and pos_prices.get("valid"):
            avg_long_exit = pos_prices["long_exit_price"]
            avg_short_exit = pos_prices["short_exit_price"]
        else:
            avg_long_exit = sum(state.entry_prices_long) / len(state.entry_prices_long)
            avg_short_exit = sum(state.entry_prices_short) / len(state.entry_prices_short)

    avg_long_entry = sum(state.entry_prices_long) / len(state.entry_prices_long)
    avg_short_entry = sum(state.entry_prices_short) / len(state.entry_prices_short)

    pnl_long = (avg_long_exit - avg_long_entry) * total_volume
    pnl_short = (avg_short_entry - avg_short_exit) * total_volume
    total_pnl = pnl_long + pnl_short

    await db.update_pair_pnl(state.pair_id, total_pnl)
    await db.log_trade_event(state.pair_id, "EXIT_OK", "info", f"Full exit, PnL={total_pnl:.2f}$", {"pnl": total_pnl, "total_volume": total_volume})
    await db.delete_position(state.pair_id)

    logger.info(f"[{state.pair_id}] EXIT COMPLETED | PnL={total_pnl:.2f}$, pair returned to READY")

    state.reset_after_exit()
    state.status = STATE_READY


# ==============================
# Main entry point
# ==============================

async def main():
    core = TradingCore()
    await core.init()  # Async initialization
    try:
        await core.start()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt in main()")
        core.shutdown_mgr.request_shutdown()
    finally:
        await core.stop()


if __name__ == "__main__":
    asyncio.run(main())
