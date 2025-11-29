# run_web.py
# ---------------------------------------------------
# Unified launcher for Trading Bot + Web Interface
# Runs FastAPI server with integrated TradingCore
# ---------------------------------------------------

import asyncio
import signal
import sys
import os
from contextlib import asynccontextmanager

import uvicorn
from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_manager import DBManager
from api_server import app, trading_core, ws_manager, broadcast_pair_update, broadcast_notification

# Import main.py's TradingCore
from main import TradingCore


# ============================================================
# CONFIGURATION
# ============================================================

WEB_HOST = "0.0.0.0"
WEB_PORT = 8080

# ============================================================
# GLOBAL STATE
# ============================================================

_trading_core: TradingCore = None
_shutdown_event = asyncio.Event()


# ============================================================
# LIFESPAN HANDLER
# ============================================================

@asynccontextmanager
async def lifespan(app):
    """Startup and shutdown handler."""
    global _trading_core

    logger.info("=" * 50)
    logger.info("ARB TERMINAL - STARTING")
    logger.info("=" * 50)

    # Initialize database (async)
    db = DBManager()
    await db.init_db()
    logger.info("Database initialized")

    # Create and initialize trading core (async init required)
    _trading_core = TradingCore()
    await _trading_core.init()  # Async initialization
    logger.info("Trading core initialized")

    # Link trading core to API server
    import api_server
    api_server.trading_core = _trading_core

    # Start trading core in background
    trading_task = asyncio.create_task(run_trading_core(_trading_core))
    logger.info("Trading core started")

    # Start balance update broadcast loop
    balance_task = asyncio.create_task(balance_broadcast_loop())

    yield

    # Shutdown
    logger.info("Shutting down...")

    # Stop trading core
    _trading_core.shutdown_mgr.request_shutdown()
    await _trading_core.stop()

    # Cancel tasks
    trading_task.cancel()
    balance_task.cancel()

    try:
        await asyncio.gather(trading_task, balance_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass

    logger.info("ARB TERMINAL - STOPPED")


async def run_trading_core(core: TradingCore):
    """Run trading core main loop."""
    try:
        await core.start()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"Trading core error: {e}")


async def balance_broadcast_loop():
    """Periodically broadcast balance updates to web clients."""
    while True:
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
# OVERRIDE LIFESPAN
# ============================================================

# Replace the default lifespan
app.router.lifespan_context = lifespan


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown."""
    def handle_signal(signum, frame):
        logger.warning(f"Received signal {signum}, initiating shutdown...")
        _shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


# ============================================================
# MAIN
# ============================================================

def main():
    """Main entry point."""
    logger.info(f"Starting ARB Terminal Web Server on {WEB_HOST}:{WEB_PORT}")
    logger.info(f"Open http://localhost:{WEB_PORT} in your browser")

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
