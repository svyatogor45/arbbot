# api_server.py
# ---------------------------------------------------
# FastAPI Web Server for Arbitrage Terminal
# Provides REST API + WebSocket for real-time updates
# ---------------------------------------------------

import asyncio
import json
import time
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from loguru import logger

from db_manager import DBManager
from exchange_manager import ExchangeManager
from config import EXCHANGES, TAKER_FEES

# Project root directory (for absolute paths)
PROJECT_ROOT = Path(__file__).parent.resolve()

# ============================================================
# PYDANTIC MODELS
# ============================================================

class ExchangeConnect(BaseModel):
    api_key: str
    secret_key: str
    passphrase: Optional[str] = None

class PairCreate(BaseModel):
    symbol: str
    volume: float = Field(gt=0)
    n_orders: int = Field(ge=1, le=10, default=1)
    entry_spread: float = Field(gt=0)
    exit_spread: float = Field(gt=0)
    stop_loss: Optional[float] = Field(ge=0, default=None)
    leverage: int = Field(ge=1, le=100, default=10)

class PairUpdate(BaseModel):
    volume: Optional[float] = Field(gt=0, default=None)
    n_orders: Optional[int] = Field(ge=1, le=10, default=None)
    entry_spread: Optional[float] = Field(gt=0, default=None)
    exit_spread: Optional[float] = Field(gt=0, default=None)
    stop_loss: Optional[float] = Field(ge=0, default=None)
    leverage: Optional[int] = Field(ge=1, le=100, default=None)

class SettingsUpdate(BaseModel):
    max_open_positions: Optional[int] = Field(ge=1, le=20, default=None)
    default_leverage: Optional[int] = Field(ge=1, le=100, default=None)
    max_monitored_pairs: Optional[int] = Field(ge=1, le=50, default=None)

# ============================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================

class ConnectionManager:
    """Manages WebSocket connections for real-time updates."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Send message to all connected clients."""
        if not self.active_connections:
            return

        data = json.dumps(message, default=str)
        async with self._lock:
            dead = []
            for conn in self.active_connections:
                try:
                    await conn.send_text(data)
                except Exception:
                    dead.append(conn)
            for conn in dead:
                self.active_connections.discard(conn)

# ============================================================
# GLOBAL STATE
# ============================================================

# These will be initialized lazily or via lifespan
db: DBManager = None
ex_manager: ExchangeManager = None
ws_manager = ConnectionManager()
trading_core = None  # Will be set by run_web.py

# Balance cache with thread-safe lock
_balance_cache: Dict[str, dict] = {}
_balance_cache_time: float = 0
_balance_cache_lock = asyncio.Lock()
BALANCE_CACHE_TTL = 60  # seconds


def _ensure_managers():
    """Lazy initialization of DB and Exchange managers."""
    global db, ex_manager
    if db is None:
        db = DBManager()
    if ex_manager is None:
        ex_manager = ExchangeManager()

# ============================================================
# LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("API Server starting...")
    # Ensure managers are initialized
    _ensure_managers()
    # Initialize exchange manager
    await ex_manager.initialize()
    yield
    logger.info("API Server shutting down...")
    if ex_manager:
        await ex_manager.close_all()

# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(
    title="Arbitrage Terminal API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files (use absolute path)
app.mount("/static", StaticFiles(directory=str(PROJECT_ROOT / "web" / "static")), name="static")

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

async def get_exchange_balance(exchange: str) -> Optional[float]:
    """Get balance for exchange with caching (thread-safe)."""
    global _balance_cache, _balance_cache_time

    now = time.time()

    # Check cache first (with lock)
    async with _balance_cache_lock:
        if now - _balance_cache_time < BALANCE_CACHE_TTL and exchange in _balance_cache:
            return _balance_cache.get(exchange, {}).get("balance")
        cached_balance = _balance_cache.get(exchange, {}).get("balance")

    # Fetch new balance (outside lock to avoid blocking)
    try:
        _ensure_managers()
        balance = await ex_manager.get_balance(exchange, "USDT")
        if balance is not None:
            async with _balance_cache_lock:
                _balance_cache[exchange] = {"balance": balance, "time": now}
                _balance_cache_time = now
        return balance
    except Exception as e:
        logger.warning(f"Failed to get balance for {exchange}: {e}")
        return cached_balance


async def refresh_all_balances() -> Dict[str, float]:
    """Refresh balances for all connected exchanges (thread-safe)."""
    global _balance_cache, _balance_cache_time

    _ensure_managers()
    connected = await db.get_connected_exchanges()
    balances = {}

    tasks = [get_exchange_balance(ex) for ex in connected]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    async with _balance_cache_lock:
        for ex, result in zip(connected, results):
            if isinstance(result, Exception):
                balances[ex] = _balance_cache.get(ex, {}).get("balance", 0)
            else:
                balances[ex] = result or 0
        _balance_cache_time = time.time()

    return balances

def format_pair_response(pair: dict, state: dict = None) -> dict:
    """Format pair data for API response."""
    response = {
        "id": pair["id"],
        "symbol": pair["symbol"],
        "volume": pair["volume"],
        "n_orders": pair["n_orders"],
        "entry_spread": pair["entry_spread"],
        "exit_spread": pair["exit_spread"],
        "stop_loss": pair.get("stop_loss"),
        "leverage": pair.get("leverage", 10),
        "status": pair["status"],
        "total_pnl": pair.get("total_pnl", 0),
        "sl_count": pair.get("sl_count", 0),
        "liq_count": pair.get("liq_count", 0),
        "created_at": pair.get("created_at"),
    }

    # Add runtime state if available
    if state:
        response.update({
            "runtime_status": state.get("status"),
            "has_position": state.get("has_position", False),
            "long_exchange": state.get("long_exchange"),
            "short_exchange": state.get("short_exchange"),
            "filled_parts": state.get("filled_parts", 0),
            "open_parts": state.get("open_parts", 0),
            "open_volume": state.get("open_volume", 0),
            "actual_long_volume": state.get("actual_long_volume", 0),
            "actual_short_volume": state.get("actual_short_volume", 0),
            "avg_entry_long": state.get("avg_entry_long", 0),
            "avg_entry_short": state.get("avg_entry_short", 0),
        })

    return response

# ============================================================
# ROUTES: PAGES
# ============================================================

@app.get("/")
async def serve_index():
    """Serve main HTML page."""
    return FileResponse(str(PROJECT_ROOT / "web" / "index.html"))

# ============================================================
# ROUTES: EXCHANGES
# ============================================================

@app.get("/api/exchanges")
async def get_exchanges():
    """Get list of all supported exchanges with connection status."""
    connected = set(await db.get_connected_exchanges())
    result = []

    for ex in EXCHANGES:
        ex_lower = ex.lower()
        is_connected = ex_lower in connected
        balance = None

        if is_connected:
            balance = await get_exchange_balance(ex_lower)

        result.append({
            "name": ex_lower,
            "display_name": ex.upper(),
            "connected": is_connected,
            "balance": balance,
            "taker_fee": TAKER_FEES.get(ex_lower, 0.06),
        })

    return result

@app.post("/api/exchanges/{name}/connect")
async def connect_exchange(name: str, data: ExchangeConnect):
    """Connect an exchange with API credentials."""
    name = name.lower()
    if name not in [e.lower() for e in EXCHANGES]:
        raise HTTPException(400, f"Unknown exchange: {name}")

    # Test connection
    try:
        success = await ex_manager.add_exchange(
            name,
            data.api_key,
            data.secret_key,
            data.passphrase
        )
        if not success:
            raise HTTPException(400, "Failed to connect to exchange")

        # Test by getting balance
        balance = await ex_manager.get_balance(name, "USDT")

        # Save to DB
        await db.save_api_credentials(name, data.api_key, data.secret_key, data.passphrase)

        # Broadcast update
        await ws_manager.broadcast({
            "type": "exchangeUpdate",
            "exchange": name,
            "connected": True,
            "balance": balance
        })

        return {"success": True, "balance": balance}

    except Exception as e:
        logger.error(f"Failed to connect {name}: {e}")
        raise HTTPException(400, f"Connection failed: {str(e)}")

@app.delete("/api/exchanges/{name}/connect")
async def disconnect_exchange(name: str):
    """Disconnect an exchange."""
    name = name.lower()

    # Check if exchange has active positions
    if trading_core:
        check = trading_core.can_remove_exchange(name)
        if not check["can_remove"]:
            raise HTTPException(400, f"Cannot disconnect: {check['reason']}")

    # Remove from DB
    await db.delete_api_credentials(name)

    # Remove from exchange manager
    await ex_manager.remove_exchange(name)

    # Broadcast update
    await ws_manager.broadcast({
        "type": "exchangeUpdate",
        "exchange": name,
        "connected": False,
        "balance": None
    })

    return {"success": True}

@app.post("/api/exchanges/refresh-balances")
async def refresh_balances():
    """Force refresh all exchange balances."""
    global _balance_cache_time
    async with _balance_cache_lock:
        _balance_cache_time = 0  # Invalidate cache

    balances = await refresh_all_balances()

    # Broadcast updates
    for ex, balance in balances.items():
        await ws_manager.broadcast({
            "type": "balanceUpdate",
            "exchange": ex,
            "balance": balance
        })

    return balances

# ============================================================
# ROUTES: PAIRS
# ============================================================

@app.get("/api/pairs")
async def get_pairs():
    """Get all trading pairs with their states."""
    pairs = await db.get_all_pairs()
    result = []

    for pair in pairs:
        state = None
        if trading_core:
            state = trading_core.get_pair_state(pair["id"])
        result.append(format_pair_response(pair, state))

    return result

@app.post("/api/pairs")
async def create_pair(data: PairCreate):
    """Add a new trading pair."""
    # Check pair limit
    existing = await db.get_all_pairs()
    if len(existing) >= 30:
        raise HTTPException(400, "Maximum 30 pairs allowed")

    # Verify symbol exists on at least 2 connected exchanges
    connected = await db.get_connected_exchanges()
    if len(connected) < 2:
        raise HTTPException(400, "Need at least 2 connected exchanges")

    # Create pair
    try:
        pair_id = await db.add_pair(
            symbol=data.symbol.upper(),
            volume=data.volume,
            n_orders=data.n_orders,
            entry_spread=data.entry_spread,
            exit_spread=data.exit_spread,
            stop_loss=data.stop_loss,
            leverage=data.leverage
        )

        pair = await db.get_pair_by_id(pair_id)

        # Broadcast update
        await ws_manager.broadcast({
            "type": "pairCreated",
            "pair": format_pair_response(pair)
        })

        return format_pair_response(pair)

    except Exception as e:
        logger.error(f"Failed to create pair: {e}")
        raise HTTPException(400, str(e))

@app.get("/api/pairs/{pair_id}")
async def get_pair(pair_id: int):
    """Get single pair details."""
    pair = await db.get_pair_by_id(pair_id)
    if not pair:
        raise HTTPException(404, "Pair not found")

    state = None
    if trading_core:
        state = trading_core.get_pair_state(pair_id)

    return format_pair_response(pair, state)

@app.patch("/api/pairs/{pair_id}")
async def update_pair(pair_id: int, data: PairUpdate):
    """Update pair parameters."""
    pair = await db.get_pair_by_id(pair_id)
    if not pair:
        raise HTTPException(404, "Pair not found")

    # Build update dict
    updates = {}
    if data.volume is not None:
        updates["volume"] = data.volume
    if data.n_orders is not None:
        updates["n_orders"] = data.n_orders
    if data.entry_spread is not None:
        updates["entry_spread"] = data.entry_spread
    if data.exit_spread is not None:
        updates["exit_spread"] = data.exit_spread
    if data.stop_loss is not None:
        updates["stop_loss"] = data.stop_loss
    if data.leverage is not None:
        updates["leverage"] = data.leverage

    if updates:
        await db.update_pair(pair_id, **updates)

    pair = await db.get_pair_by_id(pair_id)
    state = trading_core.get_pair_state(pair_id) if trading_core else None

    # Broadcast update
    await ws_manager.broadcast({
        "type": "pairUpdate",
        "pair": format_pair_response(pair, state)
    })

    return format_pair_response(pair, state)

@app.post("/api/pairs/{pair_id}/start")
async def start_pair(pair_id: int):
    """Start monitoring for a pair."""
    pair = await db.get_pair_by_id(pair_id)
    if not pair:
        raise HTTPException(404, "Pair not found")

    await db.update_pair_status(pair_id, "active")

    pair = await db.get_pair_by_id(pair_id)
    state = trading_core.get_pair_state(pair_id) if trading_core else None

    # Broadcast update
    await ws_manager.broadcast({
        "type": "pairUpdate",
        "pair": format_pair_response(pair, state)
    })

    return {"success": True, "status": "active"}

@app.post("/api/pairs/{pair_id}/pause")
async def pause_pair(pair_id: int):
    """Pause monitoring for a pair."""
    pair = await db.get_pair_by_id(pair_id)
    if not pair:
        raise HTTPException(404, "Pair not found")

    await db.update_pair_status(pair_id, "paused")

    pair = await db.get_pair_by_id(pair_id)
    state = trading_core.get_pair_state(pair_id) if trading_core else None

    # Broadcast update
    await ws_manager.broadcast({
        "type": "pairUpdate",
        "pair": format_pair_response(pair, state)
    })

    return {"success": True, "status": "paused"}

@app.post("/api/pairs/{pair_id}/close")
async def close_pair_position(pair_id: int):
    """Force close position for a pair."""
    if not trading_core:
        raise HTTPException(503, "Trading core not available")

    result = await trading_core.force_close_position(pair_id)

    if result["success"]:
        pair = await db.get_pair_by_id(pair_id)
        state = trading_core.get_pair_state(pair_id)

        # Broadcast update
        await ws_manager.broadcast({
            "type": "pairUpdate",
            "pair": format_pair_response(pair, state)
        })
        await ws_manager.broadcast({
            "type": "notification",
            "notif": {
                "timestamp": datetime.now().isoformat(),
                "type": "CLOSE",
                "severity": "info",
                "pairId": pair_id,
                "message": f"Position closed. PnL: {result.get('pnl', 0):.2f} USDT"
            }
        })

    return result

@app.delete("/api/pairs/{pair_id}")
async def delete_pair(pair_id: int, force: bool = False):
    """Delete a trading pair."""
    pair = await db.get_pair_by_id(pair_id)
    if not pair:
        raise HTTPException(404, "Pair not found")

    if trading_core:
        result = await trading_core.delete_pair(pair_id, force=force)
        if not result["success"]:
            raise HTTPException(400, result.get("error", "Delete failed"))
    else:
        await db.delete_pair(pair_id)

    # Broadcast update
    await ws_manager.broadcast({
        "type": "pairDeleted",
        "pairId": pair_id
    })

    return {"success": True}

# ============================================================
# ROUTES: STATISTICS
# ============================================================

@app.get("/api/stats")
async def get_stats():
    """Get aggregated statistics."""
    stats = await db.get_statistics()

    # Get top pairs
    top_profitable = await db.get_top_pairs_by_pnl(limit=5, ascending=False)
    top_losing = await db.get_top_pairs_by_pnl(limit=5, ascending=True)
    top_trades = await db.get_top_pairs_by_trades(limit=5)

    return {
        "totals": stats,
        "top_profitable": top_profitable,
        "top_losing": top_losing,
        "top_by_trades": top_trades,
    }

@app.post("/api/stats/reset")
async def reset_stats():
    """Reset statistics."""
    await db.reset_statistics()

    await ws_manager.broadcast({
        "type": "statsUpdate",
        "stats": await db.get_statistics()
    })

    return {"success": True}

# ============================================================
# ROUTES: SETTINGS
# ============================================================

@app.get("/api/settings")
async def get_settings():
    """Get all bot settings."""
    if trading_core:
        return await trading_core.get_settings()

    return {
        "max_open_positions": {
            "value": await db.get_setting_int("max_open_positions", 5),
            "description": "Maximum concurrent arbitrages",
            "min": 1,
            "max": 20,
        },
        "default_leverage": {
            "value": await db.get_setting_int("default_leverage", 10),
            "description": "Default leverage for all exchanges",
            "min": 1,
            "max": 100,
        },
        "max_monitored_pairs": {
            "value": await db.get_setting_int("max_monitored_pairs", 30),
            "description": "Maximum monitored pairs",
            "min": 1,
            "max": 50,
        },
    }

@app.patch("/api/settings")
async def update_settings(data: SettingsUpdate):
    """Update bot settings."""
    updates = {}

    if data.max_open_positions is not None:
        await db.set_setting("max_open_positions", str(data.max_open_positions))
        updates["max_open_positions"] = data.max_open_positions

    if data.default_leverage is not None:
        await db.set_setting("default_leverage", str(data.default_leverage))
        updates["default_leverage"] = data.default_leverage

    if data.max_monitored_pairs is not None:
        await db.set_setting("max_monitored_pairs", str(data.max_monitored_pairs))
        updates["max_monitored_pairs"] = data.max_monitored_pairs

    # FIX: Сброс кэша настроек — новые значения применяются МГНОВЕННО
    if updates and trading_core:
        trading_core.risk_controller.invalidate_cache()

    # Broadcast update
    await ws_manager.broadcast({
        "type": "settingsUpdate",
        "settings": updates
    })

    return {"success": True, "updated": updates}

# ============================================================
# ROUTES: NOTIFICATIONS / EVENTS
# ============================================================

@app.get("/api/notifications")
async def get_notifications(
    types: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Get trade events / notifications."""
    type_filter = types.split(",") if types else None
    events = await db.get_trade_events(limit=limit, offset=offset, event_types=type_filter)

    return {
        "notifications": events,
        "total": await db.count_trade_events(event_types=type_filter)
    }

@app.delete("/api/notifications")
async def clear_notifications():
    """Clear all notifications."""
    await db.clear_trade_events()
    return {"success": True}

# ============================================================
# WEBSOCKET
# ============================================================

@app.websocket("/ws/stream")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await ws_manager.connect(websocket)

    try:
        # Send initial state
        pairs = await db.get_all_pairs()
        pair_data = []
        for pair in pairs:
            state = trading_core.get_pair_state(pair["id"]) if trading_core else None
            pair_data.append(format_pair_response(pair, state))

        await websocket.send_json({
            "type": "init",
            "pairs": pair_data,
            "balances": await refresh_all_balances(),
            "settings": await get_settings(),
        })

        # Keep connection alive and handle messages
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                msg = json.loads(data)

                # Handle ping
                if msg.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})

            except asyncio.TimeoutError:
                # Send ping to keep alive
                await websocket.send_json({"type": "ping"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await ws_manager.disconnect(websocket)

# ============================================================
# BROADCAST HELPERS (called from TradingCore)
# ============================================================

async def broadcast_pair_update(pair_id: int, pair: dict, state: dict = None):
    """Broadcast pair update to all clients."""
    await ws_manager.broadcast({
        "type": "pairUpdate",
        "pair": format_pair_response(pair, state)
    })

async def broadcast_notification(notif: dict):
    """Broadcast notification to all clients."""
    await ws_manager.broadcast({
        "type": "notification",
        "notif": notif
    })

async def broadcast_balance_update(exchange: str, balance: float):
    """Broadcast balance update."""
    await ws_manager.broadcast({
        "type": "balanceUpdate",
        "exchange": exchange,
        "balance": balance
    })

# ============================================================
# ENTRY POINT (for standalone testing)
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
