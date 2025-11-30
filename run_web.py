# run_web.py
# ---------------------------------------------------
# Unified launcher for Trading Bot + Web Interface
# Includes: TradingCore supervisor with auto-restart
#
# Features (merged from launcher.py):
#   - Auto-restart on crash
#   - Crash limit protection (5 per 60s)
#   - Exponential backoff
#   - Emergency positions check on startup
#   - Supervisor metrics
#   - Web broadcast on crash/restart
# ---------------------------------------------------

import asyncio
import signal
import sys
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass, field

import uvicorn
from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_manager import DBManager
from exchange_manager import ExchangeManager
import api_server
from api_server import app, ws_manager

# Import main.py's TradingCore
from main import TradingCore


# ============================================================
# CONFIGURATION
# ============================================================

WEB_HOST = "0.0.0.0"
WEB_PORT = 8080

# Supervisor settings (from launcher.py)
RESTART_LIMIT = 5           # Max restarts in window
RESTART_WINDOW = 60         # seconds
RESTART_DELAY_BASE = 5.0    # seconds
RESTART_DELAY_MAX = 60.0    # seconds
RESTART_BACKOFF_MULTIPLIER = 1.5


# ============================================================
# SUPERVISOR METRICS
# ============================================================

@dataclass
class SupervisorMetrics:
    """Метрики работы supervisor."""
    started_at: datetime = field(default_factory=datetime.now)
    total_starts: int = 0
    total_crashes: int = 0
    last_start_at: Optional[datetime] = None
    last_crash_at: Optional[datetime] = None
    last_crash_reason: str = ""
    uptime_seconds: float = 0.0
    restart_times: List[float] = field(default_factory=list)
    core_start_time: float = 0.0

    def record_start(self):
        self.total_starts += 1
        self.last_start_at = datetime.now()
        self.core_start_time = time.time()

    def record_crash(self, reason: str = ""):
        self.total_crashes += 1
        self.last_crash_at = datetime.now()
        self.last_crash_reason = reason
        self.restart_times.append(time.time())
        # Clean old entries
        now = time.time()
        self.restart_times = [t for t in self.restart_times if now - t < RESTART_WINDOW]

    def update_uptime(self):
        if self.core_start_time > 0:
            self.uptime_seconds = time.time() - self.core_start_time

    @property
    def crashes_in_window(self) -> int:
        now = time.time()
        return len([t for t in self.restart_times if now - t < RESTART_WINDOW])

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at.isoformat(),
            "total_starts": self.total_starts,
            "total_crashes": self.total_crashes,
            "crashes_in_window": self.crashes_in_window,
            "last_start": self.last_start_at.isoformat() if self.last_start_at else None,
            "last_crash": self.last_crash_at.isoformat() if self.last_crash_at else None,
            "last_crash_reason": self.last_crash_reason,
            "uptime_seconds": round(self.uptime_seconds, 2),
            "restart_limit": RESTART_LIMIT,
            "restart_window": RESTART_WINDOW,
        }


# ============================================================
# GLOBAL STATE
# ============================================================

_trading_core: Optional[TradingCore] = None
_shutdown_event = asyncio.Event()
_supervisor_metrics = SupervisorMetrics()
_supervisor_task: Optional[asyncio.Task] = None


# ============================================================
# STARTUP CHECKS (from launcher.py)
# ============================================================

async def startup_checks(db: DBManager) -> tuple:
    """
    Check positions and emergency positions on startup.
    Returns (positions_count, emergency_count).
    """
    positions_count = 0
    emergency_count = 0

    try:
        # Check positions
        rows = await db.load_all_positions()

        if not rows:
            logger.info("✅ В таблице positions нет активных позиций — чистый запуск.")
        else:
            logger.warning(f"⚠️ Найдено {len(rows)} позиций для восстановления:")
            for pos in rows:
                filled = pos.get('filled_parts', 0)
                closed = pos.get('closed_parts', 0)
                open_parts = max(0, filled - closed)

                if open_parts > 0:
                    positions_count += 1
                    logger.warning(
                        f"  pair_id={pos.get('pair_id')} | "
                        f"open_parts={open_parts} | "
                        f"long={pos.get('long_exchange')} | "
                        f"short={pos.get('short_exchange')}"
                    )

        # Check emergency positions
        emergencies = await db.get_pending_emergency_positions()

        if emergencies:
            emergency_count = len(emergencies)
            logger.critical(
                f"🚨 ВНИМАНИЕ: Найдено {emergency_count} PENDING EMERGENCY позиций!"
            )
            for em in emergencies:
                logger.critical(
                    f"  ID={em.get('id')} | pair={em.get('pair_id')} | "
                    f"{em.get('exchange')} {em.get('side')} {em.get('amount')} | "
                    f"reason={em.get('reason')}"
                )

    except Exception as e:
        logger.error(f"Ошибка при проверке позиций: {e}")

    return positions_count, emergency_count


# ============================================================
# BROADCAST HELPERS
# ============================================================

async def broadcast_supervisor_event(event_type: str, data: dict = None):
    """Broadcast supervisor event to web clients."""
    try:
        message = {
            "type": "supervisor",
            "event": event_type,
            "data": data or {},
            "timestamp": datetime.now().isoformat(),
        }
        await ws_manager.broadcast(message)
    except Exception as e:
        logger.warning(f"Failed to broadcast supervisor event: {e}")


async def broadcast_notification(level: str, title: str, message: str):
    """Broadcast notification to web clients."""
    try:
        await ws_manager.broadcast({
            "type": "notification",
            "level": level,
            "title": title,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        })
    except Exception:
        pass


# ============================================================
# TRADING CORE SUPERVISOR
# ============================================================

def calculate_restart_delay(consecutive_crashes: int) -> float:
    """Calculate delay with exponential backoff."""
    delay = RESTART_DELAY_BASE * (RESTART_BACKOFF_MULTIPLIER ** consecutive_crashes)
    return min(delay, RESTART_DELAY_MAX)


async def run_trading_core_supervised():
    """
    Supervisor loop for TradingCore.
    Handles crashes, auto-restart, and crash limits.
    """
    global _trading_core, _supervisor_metrics

    consecutive_crashes = 0

    while not _shutdown_event.is_set():
        try:
            # Create new TradingCore instance
            _trading_core = TradingCore()
            await _trading_core.init()

            # Link to API server
            api_server.trading_core = _trading_core

            _supervisor_metrics.record_start()
            logger.info(f"🚀 TradingCore запущен (start #{_supervisor_metrics.total_starts})")

            await broadcast_supervisor_event("core_started", {
                "start_number": _supervisor_metrics.total_starts,
            })
            await broadcast_notification("success", "Trading Core", "Торговое ядро запущено")

            # Run trading core
            await _trading_core.start()

            # Normal exit
            logger.info("✅ TradingCore завершился нормально")
            consecutive_crashes = 0
            break

        except asyncio.CancelledError:
            logger.info("⛔ TradingCore отменён (shutdown)")
            break

        except Exception as e:
            # Crash!
            consecutive_crashes += 1
            error_msg = str(e)[:200]
            _supervisor_metrics.record_crash(error_msg)

            logger.exception(f"🔥 TradingCore CRASH #{consecutive_crashes}: {e}")

            await broadcast_supervisor_event("core_crashed", {
                "crash_number": _supervisor_metrics.total_crashes,
                "error": error_msg,
                "crashes_in_window": _supervisor_metrics.crashes_in_window,
            })
            await broadcast_notification(
                "error",
                "Trading Core Crash",
                f"Краш #{consecutive_crashes}: {error_msg[:100]}"
            )

            # Cleanup crashed core
            if _trading_core:
                try:
                    _trading_core.shutdown_mgr.request_shutdown()
                    await asyncio.wait_for(_trading_core.stop(), timeout=10)
                except Exception as cleanup_err:
                    logger.warning(f"Ошибка при очистке после краша: {cleanup_err}")
                _trading_core = None

            # Check crash limit
            if _supervisor_metrics.crashes_in_window >= RESTART_LIMIT:
                logger.critical(
                    f"❌ СЛИШКОМ МНОГО КРАШЕЙ ({_supervisor_metrics.crashes_in_window} "
                    f"за {RESTART_WINDOW}s)! Автоперезапуск ОТКЛЮЧЁН."
                )
                await broadcast_notification(
                    "error",
                    "Supervisor STOP",
                    f"Превышен лимит крашей ({RESTART_LIMIT} за {RESTART_WINDOW}s). "
                    "Автоперезапуск отключён для безопасности."
                )
                break

            # Calculate delay
            delay = calculate_restart_delay(consecutive_crashes - 1)
            logger.warning(
                f"🔁 Автоперезапуск через {delay:.1f}s... "
                f"(crashes: {_supervisor_metrics.crashes_in_window}/{RESTART_LIMIT})"
            )

            await broadcast_supervisor_event("core_restarting", {
                "delay_seconds": delay,
                "crashes_in_window": _supervisor_metrics.crashes_in_window,
                "restart_limit": RESTART_LIMIT,
            })

            # Wait with cancel support
            try:
                await asyncio.wait_for(
                    _shutdown_event.wait(),
                    timeout=delay
                )
                # Shutdown requested during wait
                logger.info("🛑 Shutdown requested во время ожидания рестарта")
                break
            except asyncio.TimeoutError:
                # Timeout expired, continue to restart
                pass

    # Final cleanup
    if _trading_core:
        try:
            _trading_core.shutdown_mgr.request_shutdown()
            await _trading_core.stop()
        except Exception:
            pass
        _trading_core = None

    _supervisor_metrics.update_uptime()
    logger.info(f"📊 Supervisor metrics: {_supervisor_metrics.to_dict()}")


# ============================================================
# BALANCE BROADCAST LOOP
# ============================================================

async def balance_broadcast_loop():
    """Periodically broadcast balance updates to web clients."""
    while not _shutdown_event.is_set():
        try:
            await asyncio.sleep(60)  # Every minute

            if _trading_core and ws_manager.active_connections:
                from api_server import refresh_all_balances, broadcast_balance_update

                balances = await refresh_all_balances()
                for ex, balance in balances.items():
                    await broadcast_balance_update(ex, balance)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Balance broadcast error: {e}")


# ============================================================
# LIFESPAN HANDLER
# ============================================================

@asynccontextmanager
async def lifespan(app):
    """Startup and shutdown handler."""
    global _supervisor_task

    logger.info("=" * 60)
    logger.info("🚀 ARB TERMINAL - STARTING (with Supervisor)")
    logger.info("=" * 60)
    logger.info(f"  Restart limit: {RESTART_LIMIT} crashes per {RESTART_WINDOW}s")
    logger.info(f"  Backoff: {RESTART_DELAY_BASE}s base, {RESTART_DELAY_MAX}s max")

    # Initialize database
    db = DBManager()
    await db.init_db()
    logger.info("✅ Database initialized")

    # Initialize exchange manager (lazy - connects on first use)
    ex_manager = ExchangeManager()

    # Load credentials from database
    await ex_manager.load_credentials_from_db(db)

    # Link to api_server globals (fix for API endpoints)
    api_server.db = db
    api_server.ex_manager = ex_manager

    # Startup checks (positions, emergency positions)
    positions_count, emergency_count = await startup_checks(db)

    if emergency_count > 0:
        await broadcast_notification(
            "warning",
            "Emergency Positions",
            f"Найдено {emergency_count} нерешённых emergency позиций!"
        )

    # Start supervisor (manages TradingCore lifecycle)
    _supervisor_task = asyncio.create_task(run_trading_core_supervised())
    logger.info("✅ Supervisor started")

    # Start balance broadcast loop
    balance_task = asyncio.create_task(balance_broadcast_loop())

    yield

    # Shutdown
    logger.info("🛑 Shutting down...")
    _shutdown_event.set()

    # Stop trading core
    if _trading_core:
        _trading_core.shutdown_mgr.request_shutdown()
        try:
            await asyncio.wait_for(_trading_core.stop(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("TradingCore stop timeout")

    # Cancel tasks
    if _supervisor_task:
        _supervisor_task.cancel()
    balance_task.cancel()

    try:
        await asyncio.gather(
            _supervisor_task,
            balance_task,
            return_exceptions=True
        )
    except asyncio.CancelledError:
        pass

    logger.info("=" * 60)
    logger.info("👋 ARB TERMINAL - STOPPED")
    logger.info(f"📊 Final metrics: {_supervisor_metrics.to_dict()}")
    logger.info("=" * 60)


# Override the default lifespan
app.router.lifespan_context = lifespan


# ============================================================
# API ENDPOINT: SUPERVISOR METRICS
# ============================================================

from fastapi import APIRouter

supervisor_router = APIRouter(prefix="/api/supervisor", tags=["supervisor"])


@supervisor_router.get("/metrics")
async def get_supervisor_metrics():
    """Get supervisor metrics."""
    _supervisor_metrics.update_uptime()
    return {
        "status": "ok",
        "metrics": _supervisor_metrics.to_dict(),
        "core_running": _trading_core is not None and not _shutdown_event.is_set(),
    }


@supervisor_router.post("/restart")
async def restart_trading_core():
    """Manually restart trading core."""
    global _trading_core

    if _trading_core:
        logger.warning("🔄 Manual restart requested via API")
        _trading_core.shutdown_mgr.request_shutdown()
        await broadcast_notification("info", "Restart", "Ручной перезапуск торгового ядра...")
        return {"status": "ok", "message": "Restart initiated"}

    return {"status": "error", "message": "Trading core not running"}


# Add supervisor routes to app
app.include_router(supervisor_router)


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown."""
    def handle_signal(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.warning(f"🛑 Received signal {sig_name}, initiating shutdown...")
        _shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


# ============================================================
# MAIN
# ============================================================

def main():
    """Main entry point."""
    logger.info(f"Starting ARB Terminal on http://{WEB_HOST}:{WEB_PORT}")
    logger.info("Features: Trading Core + Web UI + Auto-Restart Supervisor")

    setup_signal_handlers()

    config = uvicorn.Config(
        app,
        host=WEB_HOST,
        port=WEB_PORT,
        log_level="info",
        access_log=False,
    )

    server = uvicorn.Server(config)

    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


if __name__ == "__main__":
    main()
