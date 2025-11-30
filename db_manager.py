# db_manager.py
# ---------------------------------------------------
# ASYNC v4.0 - Pure async, no sync wrappers
# Performance: Non-blocking DB operations
# ---------------------------------------------------

import aiosqlite
import asyncio
import json
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

from config import DB_NAME, logger


# ============================================================
# CONFIGURATION
# ============================================================

MAX_BUSY_RETRIES = 5
BUSY_RETRY_DELAY = 0.05
CONNECTION_TIMEOUT = 10.0


# ============================================================
# METRICS
# ============================================================

@dataclass
class DBMetrics:
    queries_total: int = 0
    queries_success: int = 0
    queries_failed: int = 0
    busy_retries: int = 0

    def record_query(self, success: bool, retries: int = 0):
        self.queries_total += 1
        if success:
            self.queries_success += 1
        else:
            self.queries_failed += 1
        self.busy_retries += retries

    def to_dict(self) -> dict:
        return {
            "queries_total": self.queries_total,
            "queries_success": self.queries_success,
            "queries_failed": self.queries_failed,
            "success_rate": f"{(self.queries_success / self.queries_total * 100):.1f}%" if self.queries_total > 0 else "N/A",
            "busy_retries": self.busy_retries,
        }


class DBManager:
    """
    Pure async SQLite manager using aiosqlite.
    All methods are async - use with await.
    """

    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        self.metrics = DBMetrics()
        self._initialized = False

    async def _ensure_connection(self) -> aiosqlite.Connection:
        """Get or create async connection."""
        if self._conn is None or not self._initialized:
            async with self._lock:
                if self._conn is None:
                    self._conn = await aiosqlite.connect(
                        self.db_name,
                        timeout=CONNECTION_TIMEOUT,
                    )
                    self._conn.row_factory = aiosqlite.Row

                    # Optimizations
                    await self._conn.execute("PRAGMA journal_mode = WAL;")
                    await self._conn.execute("PRAGMA synchronous = NORMAL;")
                    await self._conn.execute("PRAGMA cache_size = -64000;")
                    await self._conn.execute("PRAGMA temp_store = MEMORY;")
                    await self._conn.execute("PRAGMA busy_timeout = 1000;")  # FIX 4.1: было 5000

                    self._initialized = True
        return self._conn

    async def _execute(self, sql: str, params: tuple = ()) -> bool:
        """Execute SQL with retry logic."""
        retries = 0
        while True:
            try:
                conn = await self._ensure_connection()
                await conn.execute(sql, params)
                await conn.commit()
                self.metrics.record_query(True, retries)
                return True
            except Exception as e:
                error_msg = str(e).lower()
                if "locked" in error_msg or "busy" in error_msg:
                    retries += 1
                    if retries >= MAX_BUSY_RETRIES:
                        self.metrics.record_query(False, retries)
                        logger.error(f"DB BUSY after {retries} retries: {e}")
                        return False
                    await asyncio.sleep(BUSY_RETRY_DELAY * (2 ** (retries - 1)))
                    continue
                self.metrics.record_query(False, retries)
                logger.error(f"DB EXEC ERROR: {e} | SQL={sql[:200]}")
                return False

    async def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Fetch all rows."""
        try:
            conn = await self._ensure_connection()
            async with conn.execute(sql, params) as cursor:
                rows = await cursor.fetchall()
                self.metrics.record_query(True)
                return [dict(r) for r in rows]
        except Exception as e:
            self.metrics.record_query(False)
            logger.error(f"DB FETCHALL ERROR: {e} | SQL={sql[:200]}")
            return []

    async def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Fetch one row."""
        try:
            conn = await self._ensure_connection()
            async with conn.execute(sql, params) as cursor:
                row = await cursor.fetchone()
                self.metrics.record_query(True)
                return dict(row) if row else None
        except Exception as e:
            self.metrics.record_query(False)
            logger.error(f"DB FETCHONE ERROR: {e} | SQL={sql[:200]}")
            return None

    async def _fetchval(self, sql: str, params: tuple = ()) -> Any:
        """Fetch single value."""
        row = await self._fetchone(sql, params)
        if row:
            return list(row.values())[0]
        return None

    # ============================================================
    # TRADING PAIRS
    # ============================================================

    async def get_active_pairs(self) -> List[Dict[str, Any]]:
        """Get all active trading pairs."""
        return await self._fetchall("SELECT * FROM trading_pairs WHERE status = 'active'")

    async def get_connected_exchanges(self) -> List[str]:
        """Get list of exchanges that have API credentials configured."""
        rows = await self._fetchall(
            "SELECT exchange FROM api_credentials WHERE api_key IS NOT NULL AND api_key != ''"
        )
        return [r["exchange"].lower() for r in rows] if rows else []

    # ============================================================
    # BOT SETTINGS
    # ============================================================

    async def get_setting(self, key: str, default: str = None) -> Optional[str]:
        """Get a single setting value by key."""
        row = await self._fetchone("SELECT value FROM bot_settings WHERE key = ?", (key,))
        return row["value"] if row else default

    async def get_setting_int(self, key: str, default: int = 0) -> int:
        """Get setting as integer."""
        val = await self.get_setting(key)
        try:
            return int(val) if val else default
        except (ValueError, TypeError):
            return default

    async def set_setting(self, key: str, value: str) -> bool:
        """Set a setting value (upsert)."""
        return await self._execute(
            """INSERT OR REPLACE INTO bot_settings (key, value, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)""",
            (key, str(value))
        )

    async def get_all_settings(self) -> Dict[str, Any]:
        """Get all settings as dict."""
        rows = await self._fetchall("SELECT key, value, description FROM bot_settings")
        if not rows:
            return {}
        return {
            r["key"]: {"value": r["value"], "description": r["description"]}
            for r in rows
        }

    async def update_settings(self, settings: Dict[str, str]) -> bool:
        """Update multiple settings at once."""
        for key, value in settings.items():
            await self.set_setting(key, value)
        return True

    async def get_pair(self, pair_id: int) -> Optional[Dict[str, Any]]:
        """Get single pair by ID."""
        return await self._fetchone("SELECT * FROM trading_pairs WHERE id = ?", (pair_id,))

    async def update_pair_status(self, pair_id: int, status: str) -> bool:
        """Update pair status."""
        result = await self._execute("UPDATE trading_pairs SET status = ? WHERE id = ?", (status, pair_id))
        if result:
            logger.info(f"Pair {pair_id} status -> {status}")
        return result

    async def update_pair_pnl(self, pair_id: int, pnl: float) -> bool:
        """Add PnL to pair total."""
        result = await self._execute("UPDATE trading_pairs SET total_pnl = total_pnl + ? WHERE id = ?", (pnl, pair_id))
        if result:
            logger.info(f"Pair {pair_id}: total_pnl += {pnl:.4f}")
        return result

    async def increment_sl(self, pair_id: int) -> bool:
        """Increment stop-loss counter."""
        sql = "UPDATE trading_pairs SET sl_count = sl_count + 1, last_sl_at = CURRENT_TIMESTAMP WHERE id = ?"
        result = await self._execute(sql, (pair_id,))
        if result:
            logger.warning(f"Pair {pair_id}: SL_COUNT++")
        return result

    async def increment_liq(self, pair_id: int) -> bool:
        """Increment liquidation counter."""
        sql = "UPDATE trading_pairs SET liq_count = liq_count + 1, last_liq_at = CURRENT_TIMESTAMP WHERE id = ?"
        result = await self._execute(sql, (pair_id,))
        if result:
            logger.error(f"Pair {pair_id}: LIQ_COUNT++")
        return result

    # ============================================================
    # ORDERS
    # ============================================================

    async def save_order(
        self,
        pair_id: Optional[int],
        exchange: str,
        side: str,
        price: float,
        amount: float,
        status: str,
        order_id: Optional[str] = None,
        filled: Optional[float] = None,
    ) -> bool:
        """Save order to database."""
        sql = """
            INSERT INTO orders (pair_id, exchange, side, price, amount, status, order_id, filled, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        result = await self._execute(sql, (pair_id, exchange, side, price, amount, status, order_id, filled))
        if result:
            logger.debug(f"ORDER pair={pair_id} ex={exchange} {side.upper()} price={price} amount={amount}")
        return result

    async def get_orders_by_pair(self, pair_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Get orders for a pair."""
        sql = "SELECT * FROM orders WHERE pair_id = ? ORDER BY created_at DESC LIMIT ?"
        return await self._fetchall(sql, (pair_id, limit))

    # ============================================================
    # EVENTS
    # ============================================================

    async def log_trade_event(
        self,
        pair_id: Optional[int],
        event_type: str,
        level: str = "info",
        message: str = "",
        meta: Optional[dict] = None,
    ) -> bool:
        """Log a trade event."""
        meta_json = json.dumps(meta, ensure_ascii=False) if meta else None
        sql = """
            INSERT INTO trade_events (pair_id, event_type, level, message, meta, created_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        result = await self._execute(sql, (pair_id, event_type, level, message, meta_json))

        log_fn = logger.warning if level == "warning" else logger.error if level == "error" else logger.info
        log_fn(f"EVENT [{event_type}] pair={pair_id} level={level} msg='{message[:100]}'")
        return result

    async def get_events_by_pair(self, pair_id: int, limit: int = 100, level: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get events for a pair."""
        if level:
            sql = "SELECT * FROM trade_events WHERE pair_id = ? AND level = ? ORDER BY created_at DESC LIMIT ?"
            params = (pair_id, level, limit)
        else:
            sql = "SELECT * FROM trade_events WHERE pair_id = ? ORDER BY created_at DESC LIMIT ?"
            params = (pair_id, limit)
        return await self._fetchall(sql, params)

    # ============================================================
    # POSITIONS
    # ============================================================

    async def save_position(
        self,
        pair_id: int,
        long_exchange: str,
        short_exchange: str,
        filled_parts: int,
        closed_parts: int,
        entry_prices_long: List[float],
        entry_prices_short: List[float],
        part_volume: float,
        actual_long_volume: Optional[float] = None,
        actual_short_volume: Optional[float] = None,
    ) -> bool:
        """Save or update position."""
        sql = """
            INSERT INTO positions (
                pair_id, long_exchange, short_exchange,
                filled_parts, closed_parts,
                entry_prices_long, entry_prices_short,
                part_volume, actual_long_volume, actual_short_volume,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(pair_id) DO UPDATE SET
                long_exchange=excluded.long_exchange,
                short_exchange=excluded.short_exchange,
                filled_parts=excluded.filled_parts,
                closed_parts=excluded.closed_parts,
                entry_prices_long=excluded.entry_prices_long,
                entry_prices_short=excluded.entry_prices_short,
                part_volume=excluded.part_volume,
                actual_long_volume=excluded.actual_long_volume,
                actual_short_volume=excluded.actual_short_volume,
                updated_at=CURRENT_TIMESTAMP
        """
        params = (
            pair_id, long_exchange, short_exchange,
            filled_parts, closed_parts,
            json.dumps(entry_prices_long), json.dumps(entry_prices_short),
            part_volume, actual_long_volume, actual_short_volume,
        )
        return await self._execute(sql, params)

    async def delete_position(self, pair_id: int) -> bool:
        """Delete position by pair ID."""
        return await self._execute("DELETE FROM positions WHERE pair_id = ?", (pair_id,))

    async def delete_pair(self, pair_id: int) -> bool:
        """Delete pair and its position."""
        await self._execute("DELETE FROM positions WHERE pair_id = ?", (pair_id,))
        return await self._execute("DELETE FROM trading_pairs WHERE id = ?", (pair_id,))

    async def load_position(self, pair_id: int) -> Optional[Dict[str, Any]]:
        """Load position by pair ID."""
        row = await self._fetchone("SELECT * FROM positions WHERE pair_id = ?", (pair_id,))
        return self._parse_position_row(row) if row else None

    async def load_all_positions(self) -> List[Dict[str, Any]]:
        """Load all positions."""
        rows = await self._fetchall("SELECT * FROM positions")
        return [self._parse_position_row(r) for r in rows]

    def _parse_position_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Parse position row JSON fields."""
        try:
            row["entry_prices_long"] = json.loads(row.get("entry_prices_long") or "[]")
        except (json.JSONDecodeError, TypeError):
            row["entry_prices_long"] = []
        try:
            row["entry_prices_short"] = json.loads(row.get("entry_prices_short") or "[]")
        except (json.JSONDecodeError, TypeError):
            row["entry_prices_short"] = []
        return row

    # ============================================================
    # EMERGENCY POSITIONS
    # ============================================================

    async def save_emergency_position(
        self,
        pair_id: int,
        exchange: str,
        symbol: str,
        side: str,
        amount: float,
        reason: str,
        meta: Optional[dict] = None,
    ) -> bool:
        """Save emergency position for manual resolution."""
        meta_json = json.dumps(meta, ensure_ascii=False) if meta else None
        sql = """
            INSERT INTO emergency_positions (
                pair_id, exchange, symbol, side, amount,
                reason, meta, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
        """
        result = await self._execute(sql, (pair_id, exchange, symbol, side, amount, reason, meta_json))
        if result:
            logger.critical(f"EMERGENCY POSITION SAVED | pair={pair_id} {exchange} {side} {amount} {symbol}")
        return result

    async def get_pending_emergency_positions(self) -> List[Dict[str, Any]]:
        """Get all pending emergency positions."""
        sql = "SELECT * FROM emergency_positions WHERE status = 'pending' ORDER BY created_at ASC"
        rows = await self._fetchall(sql)
        for r in rows:
            try:
                r["meta"] = json.loads(r.get("meta") or "{}")
            except (json.JSONDecodeError, TypeError):
                r["meta"] = {}
        return rows

    async def resolve_emergency_position(self, emergency_id: int, resolution: str, resolved_by: str = "system") -> bool:
        """Mark emergency position as resolved."""
        sql = """
            UPDATE emergency_positions
            SET status = 'resolved', resolution = ?, resolved_by = ?, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """
        return await self._execute(sql, (resolution, resolved_by, emergency_id))

    # ============================================================
    # RESTORE AFTER RESTART
    # ============================================================

    async def get_open_positions_for_restore(self) -> List[Dict[str, Any]]:
        """Get open positions for restore after restart."""
        sql = """
            SELECT
                p.pair_id, p.long_exchange, p.short_exchange,
                p.filled_parts, p.closed_parts,
                p.entry_prices_long, p.entry_prices_short,
                p.part_volume, p.actual_long_volume, p.actual_short_volume,
                tp.symbol, tp.volume AS total_volume, tp.n_orders,
                tp.entry_spread, tp.exit_spread, tp.stop_loss
            FROM positions p
            JOIN trading_pairs tp ON tp.id = p.pair_id
        """
        rows = await self._fetchall(sql)

        result = []
        for r in rows:
            try:
                entry_prices_long = json.loads(r.get("entry_prices_long") or "[]")
            except (json.JSONDecodeError, TypeError):
                entry_prices_long = []
            try:
                entry_prices_short = json.loads(r.get("entry_prices_short") or "[]")
            except (json.JSONDecodeError, TypeError):
                entry_prices_short = []

            filled_parts = int(r.get("filled_parts") or 0)
            closed_parts = int(r.get("closed_parts") or 0)
            open_parts = max(0, filled_parts - closed_parts)

            if open_parts <= 0:
                continue

            n_orders = int(r.get("n_orders") or 1)
            if filled_parts < n_orders and closed_parts == 0:
                status = "ENTERING"
            elif closed_parts > 0 and open_parts > 0:
                status = "EXITING"
            else:
                status = "HOLD"

            result.append({
                "pair_id": r["pair_id"],
                "symbol": r["symbol"],
                "total_volume": float(r.get("total_volume") or 0.0),
                "n_orders": n_orders,
                "entry_spread": float(r.get("entry_spread") or 0.0),
                "exit_spread": float(r.get("exit_spread") or 0.0),
                "stop_loss": float(r.get("stop_loss") or 0.0),
                "status": status,
                "long_exchange": r.get("long_exchange"),
                "short_exchange": r.get("short_exchange"),
                "filled_parts": filled_parts,
                "closed_parts": closed_parts,
                "entry_prices_long": entry_prices_long,
                "entry_prices_short": entry_prices_short,
                "exit_prices_long": [],
                "exit_prices_short": [],
                "part_volume": float(r.get("part_volume") or 0.0),
                "actual_long_volume": float(r.get("actual_long_volume") or 0.0),
                "actual_short_volume": float(r.get("actual_short_volume") or 0.0),
            })
        return result

    async def get_total_open_notional(self) -> float:
        """Calculate total open notional value."""
        rows = await self._fetchall("SELECT * FROM positions")

        total = 0.0
        for r in rows:
            try:
                entry_prices_long = json.loads(r.get("entry_prices_long") or "[]")
            except (json.JSONDecodeError, TypeError):
                entry_prices_long = []
            try:
                entry_prices_short = json.loads(r.get("entry_prices_short") or "[]")
            except (json.JSONDecodeError, TypeError):
                entry_prices_short = []

            filled_parts = int(r.get("filled_parts") or 0)
            closed_parts = int(r.get("closed_parts") or 0)
            open_parts = max(0, filled_parts - closed_parts)
            part_volume = float(r.get("part_volume") or 0.0)

            if open_parts <= 0 or part_volume <= 0:
                continue

            open_volume = open_parts * part_volume
            if not entry_prices_long and not entry_prices_short:
                continue

            avg_long = sum(entry_prices_long) / len(entry_prices_long) if entry_prices_long else 0.0
            avg_short = sum(entry_prices_short) / len(entry_prices_short) if entry_prices_short else 0.0
            avg_price = (avg_long + avg_short) / 2 if avg_long > 0 and avg_short > 0 else max(avg_long, avg_short)

            if avg_price <= 0:
                continue
            total += open_volume * avg_price
        return total

    async def get_open_positions_count(self) -> int:
        """Get count of open positions."""
        sql = "SELECT COUNT(*) FROM positions WHERE filled_parts > closed_parts"
        result = await self._fetchval(sql)
        return int(result or 0)

    async def get_trading_stats(self) -> Dict[str, Any]:
        """Get trading statistics."""
        sql = """
            SELECT
                COUNT(*) as total_pairs,
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_pairs,
                SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END) as paused_pairs,
                SUM(total_pnl) as total_pnl,
                SUM(sl_count) as total_sl,
                SUM(liq_count) as total_liq
            FROM trading_pairs
        """
        pairs_stats = await self._fetchone(sql)

        return {
            "pairs": pairs_stats or {},
            "open_positions": await self.get_open_positions_count(),
            "total_notional": round(await self.get_total_open_notional(), 2),
            "db_metrics": self.metrics.to_dict(),
        }

    async def cleanup_old_events(self, days: int = 30) -> int:
        """Cleanup old trade events."""
        sql = f"DELETE FROM trade_events WHERE created_at < datetime('now', '-{days} days')"
        await self._execute(sql)
        logger.info(f"Cleaned up old events (>{days} days)")
        return 0

    async def vacuum(self):
        """Run VACUUM on database."""
        await self._execute("VACUUM;")
        logger.info("VACUUM executed")

    def get_metrics(self) -> dict:
        """Get DB metrics (sync - no DB access)."""
        return self.metrics.to_dict()

    # ============================================================
    # API CREDENTIALS
    # ============================================================

    async def save_api_credentials(
        self,
        exchange: str,
        api_key: str,
        secret_key: str,
        passphrase: Optional[str] = None
    ) -> bool:
        """Save or update API credentials."""
        sql = """
            INSERT INTO api_credentials (exchange, api_key, secret_key, passphrase, is_active, updated_at)
            VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(exchange) DO UPDATE SET
                api_key = excluded.api_key,
                secret_key = excluded.secret_key,
                passphrase = excluded.passphrase,
                is_active = 1,
                updated_at = CURRENT_TIMESTAMP
        """
        result = await self._execute(sql, (exchange.lower(), api_key, secret_key, passphrase))
        if result:
            logger.info(f"API credentials saved for {exchange}")
        return result

    async def delete_api_credentials(self, exchange: str) -> bool:
        """Delete API credentials."""
        sql = "DELETE FROM api_credentials WHERE exchange = ?"
        result = await self._execute(sql, (exchange.lower(),))
        if result:
            logger.info(f"API credentials deleted for {exchange}")
        return result

    async def get_api_credentials(self, exchange: str) -> Optional[Dict[str, Any]]:
        """Get API credentials for exchange."""
        sql = "SELECT * FROM api_credentials WHERE exchange = ?"
        return await self._fetchone(sql, (exchange.lower(),))

    # ============================================================
    # PAIRS - ADDITIONAL METHODS FOR WEB API
    # ============================================================

    async def get_all_pairs(self) -> List[Dict[str, Any]]:
        """Get all trading pairs."""
        sql = "SELECT * FROM trading_pairs ORDER BY created_at DESC"
        return await self._fetchall(sql)

    async def get_pair_by_id(self, pair_id: int) -> Optional[Dict[str, Any]]:
        """Alias for get_pair."""
        return await self.get_pair(pair_id)

    async def add_pair(
        self,
        symbol: str,
        volume: float,
        n_orders: int,
        entry_spread: float,
        exit_spread: float,
        stop_loss: Optional[float] = None,
        leverage: int = 10,
        exchange_a: str = "",
        exchange_b: str = "",
    ) -> int:
        """Add a new trading pair. Returns pair ID."""
        sql = """
            INSERT INTO trading_pairs (
                symbol, exchange_a, exchange_b, volume, n_orders,
                entry_spread, exit_spread, stop_loss, leverage,
                status, total_pnl, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'paused', 0.0, CURRENT_TIMESTAMP)
        """
        await self._execute(sql, (
            symbol.upper(), exchange_a, exchange_b, volume, n_orders,
            entry_spread, exit_spread, stop_loss, leverage
        ))
        row = await self._fetchone("SELECT last_insert_rowid() as id")
        pair_id = row["id"] if row else 0
        logger.info(f"Pair added: {symbol} ID={pair_id}")
        return pair_id

    async def update_pair(self, pair_id: int, **kwargs) -> bool:
        """Update pair parameters."""
        allowed_fields = {"volume", "n_orders", "entry_spread", "exit_spread", "stop_loss", "leverage"}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields and v is not None}

        if not updates:
            return True

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [pair_id]
        sql = f"UPDATE trading_pairs SET {set_clause} WHERE id = ?"

        result = await self._execute(sql, tuple(values))
        if result:
            logger.info(f"Pair {pair_id} updated: {updates}")
        return result

    # ============================================================
    # STATISTICS FOR WEB API
    # ============================================================

    async def get_statistics(self) -> Dict[str, Any]:
        """Get aggregated statistics."""
        sql = """
            SELECT
                COALESCE(SUM(sl_count), 0) + COALESCE(SUM(liq_count), 0) as total_trades,
                COALESCE(SUM(total_pnl), 0) as total_pnl,
                COALESCE(SUM(sl_count), 0) as total_sl,
                COALESCE(SUM(liq_count), 0) as total_liq
            FROM trading_pairs
        """
        stats = await self._fetchone(sql)
        return stats or {"total_trades": 0, "total_pnl": 0, "total_sl": 0, "total_liq": 0}

    async def reset_statistics(self) -> bool:
        """Reset all pair statistics."""
        sql = "UPDATE trading_pairs SET total_pnl = 0, sl_count = 0, liq_count = 0"
        return await self._execute(sql)

    async def get_top_pairs_by_pnl(self, limit: int = 5, ascending: bool = False) -> List[Dict[str, Any]]:
        """Get top pairs by PnL."""
        order = "ASC" if ascending else "DESC"
        sql = f"SELECT symbol, total_pnl as pnl FROM trading_pairs ORDER BY total_pnl {order} LIMIT ?"
        return await self._fetchall(sql, (limit,))

    async def get_top_pairs_by_trades(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get most active pairs."""
        sql = """
            SELECT symbol, (sl_count + liq_count) as trades
            FROM trading_pairs
            ORDER BY (sl_count + liq_count) DESC
            LIMIT ?
        """
        return await self._fetchall(sql, (limit,))

    # ============================================================
    # TRADE EVENTS FOR WEB API
    # ============================================================

    async def get_trade_events(
        self,
        limit: int = 100,
        offset: int = 0,
        event_types: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get trade events with optional filtering."""
        if event_types:
            placeholders = ",".join("?" * len(event_types))
            sql = f"""
                SELECT * FROM trade_events
                WHERE event_type IN ({placeholders})
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """
            params = tuple(event_types) + (limit, offset)
        else:
            sql = "SELECT * FROM trade_events ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params = (limit, offset)

        rows = await self._fetchall(sql, params)

        for r in rows:
            try:
                r["meta"] = json.loads(r.get("meta") or "{}")
            except (json.JSONDecodeError, TypeError):
                r["meta"] = {}
        return rows

    async def count_trade_events(self, event_types: Optional[List[str]] = None) -> int:
        """Count trade events."""
        if event_types:
            placeholders = ",".join("?" * len(event_types))
            sql = f"SELECT COUNT(*) FROM trade_events WHERE event_type IN ({placeholders})"
            params = tuple(event_types)
        else:
            sql = "SELECT COUNT(*) FROM trade_events"
            params = ()

        result = await self._fetchval(sql, params)
        return int(result or 0)

    async def clear_trade_events(self) -> bool:
        """Clear all trade events."""
        return await self._execute("DELETE FROM trade_events")

    # ============================================================
    # DATABASE INITIALIZATION (async version of database.py)
    # ============================================================

    async def _get_table_columns(self, table_name: str) -> List[str]:
        """Get list of columns for a table."""
        rows = await self._fetchall(f"PRAGMA table_info({table_name})")
        return [row["name"] for row in rows]

    async def _ensure_column(self, table_name: str, column_name: str, column_def: str):
        """Add column if it doesn't exist."""
        cols = await self._get_table_columns(table_name)
        if column_name not in cols:
            logger.info(f"DB: adding column {table_name}.{column_name}")
            await self._execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

    async def _table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        row = await self._fetchone(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return row is not None

    async def _index_exists(self, index_name: str) -> bool:
        """Check if index exists."""
        row = await self._fetchone(
            "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
            (index_name,)
        )
        return row is not None

    async def _create_index_if_not_exists(self, index_name: str, table_name: str, columns: str):
        """Create index if it doesn't exist."""
        if not await self._index_exists(index_name):
            logger.info(f"DB: creating index {index_name}")
            await self._execute(f"CREATE INDEX {index_name} ON {table_name} ({columns})")

    async def init_db(self):
        """Initialize database schema (async version)."""
        conn = await self._ensure_connection()

        try:
            # ---------------------------------
            # 1. Exchange accounts
            # ---------------------------------
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS exchanges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    api_key TEXT,
                    secret_key TEXT,
                    passphrase TEXT,
                    is_connected BOOLEAN DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self._ensure_column("exchanges", "created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")
            await self._ensure_column("exchanges", "updated_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")

            # ---------------------------------
            # 2. Trading pairs
            # ---------------------------------
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trading_pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    exchange_a TEXT NOT NULL,
                    exchange_b TEXT NOT NULL,
                    volume REAL NOT NULL,
                    n_orders INTEGER DEFAULT 1,
                    entry_spread REAL NOT NULL,
                    exit_spread REAL NOT NULL,
                    stop_loss REAL,
                    status TEXT DEFAULT 'paused',
                    total_pnl REAL DEFAULT 0.0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self._ensure_column("trading_pairs", "sl_count", "INTEGER DEFAULT 0")
            await self._ensure_column("trading_pairs", "liq_count", "INTEGER DEFAULT 0")
            await self._ensure_column("trading_pairs", "last_sl_at", "DATETIME")
            await self._ensure_column("trading_pairs", "last_liq_at", "DATETIME")
            await self._ensure_column("trading_pairs", "created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")
            await self._ensure_column("trading_pairs", "leverage", "INTEGER DEFAULT 10")
            await self._create_index_if_not_exists("idx_trading_pairs_status", "trading_pairs", "status")
            await self._create_index_if_not_exists("idx_trading_pairs_symbol", "trading_pairs", "symbol")

            # ---------------------------------
            # 3. Orders table
            # ---------------------------------
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id INTEGER,
                    exchange TEXT,
                    side TEXT,
                    price REAL,
                    amount REAL,
                    status TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            # Migration: timestamp -> created_at
            columns = await self._get_table_columns("orders")
            if "timestamp" in columns and "created_at" not in columns:
                logger.info("DB: migrating orders.timestamp -> orders.created_at")
                await self._execute("ALTER TABLE orders ADD COLUMN created_at DATETIME")
                await self._execute("UPDATE orders SET created_at = timestamp")
                columns = await self._get_table_columns("orders")
            if "created_at" not in columns:
                await self._ensure_column("orders", "created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")
            await self._ensure_column("orders", "order_id", "TEXT")
            await self._ensure_column("orders", "filled", "REAL")
            await self._ensure_column("orders", "average_price", "REAL")
            await self._create_index_if_not_exists("idx_orders_pair_id", "orders", "pair_id")
            await self._create_index_if_not_exists("idx_orders_created_at", "orders", "created_at")
            await self._create_index_if_not_exists("idx_orders_exchange", "orders", "exchange")

            # ---------------------------------
            # 4. Trade events log
            # ---------------------------------
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id INTEGER,
                    event_type TEXT NOT NULL,
                    level TEXT DEFAULT 'info',
                    message TEXT,
                    meta TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self._create_index_if_not_exists("idx_trade_events_pair_id", "trade_events", "pair_id")
            await self._create_index_if_not_exists("idx_trade_events_event_type", "trade_events", "event_type")
            await self._create_index_if_not_exists("idx_trade_events_level", "trade_events", "level")
            await self._create_index_if_not_exists("idx_trade_events_created_at", "trade_events", "created_at")

            # ---------------------------------
            # 5. Positions table
            # ---------------------------------
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    pair_id INTEGER PRIMARY KEY,
                    long_exchange TEXT,
                    short_exchange TEXT,
                    filled_parts INTEGER DEFAULT 0,
                    closed_parts INTEGER DEFAULT 0,
                    entry_prices_long TEXT,
                    entry_prices_short TEXT,
                    part_volume REAL DEFAULT 0.0,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            position_cols = {
                "long_exchange": "TEXT",
                "short_exchange": "TEXT",
                "filled_parts": "INTEGER DEFAULT 0",
                "closed_parts": "INTEGER DEFAULT 0",
                "entry_prices_long": "TEXT",
                "entry_prices_short": "TEXT",
                "part_volume": "REAL DEFAULT 0.0",
                "updated_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
            }
            for col, definition in position_cols.items():
                await self._ensure_column("positions", col, definition)
            await self._ensure_column("positions", "actual_long_volume", "REAL")
            await self._ensure_column("positions", "actual_short_volume", "REAL")
            await self._ensure_column("positions", "exit_prices_long", "TEXT")
            await self._ensure_column("positions", "exit_prices_short", "TEXT")
            await self._ensure_column("positions", "created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")

            # ---------------------------------
            # 6. Emergency positions
            # ---------------------------------
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS emergency_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id INTEGER,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    amount REAL NOT NULL,
                    reason TEXT NOT NULL,
                    meta TEXT,
                    status TEXT DEFAULT 'pending',
                    resolution TEXT,
                    resolved_by TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    resolved_at DATETIME
                )
                """
            )
            await self._create_index_if_not_exists("idx_emergency_positions_status", "emergency_positions", "status")
            await self._create_index_if_not_exists("idx_emergency_positions_pair_id", "emergency_positions", "pair_id")
            await self._create_index_if_not_exists("idx_emergency_positions_created_at", "emergency_positions", "created_at")

            # ---------------------------------
            # 7. System metrics
            # ---------------------------------
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS system_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    metric_value REAL,
                    metric_data TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await self._create_index_if_not_exists("idx_system_metrics_name", "system_metrics", "metric_name")
            await self._create_index_if_not_exists("idx_system_metrics_created_at", "system_metrics", "created_at")

            # ---------------------------------
            # 8. API credentials
            # ---------------------------------
            if not await self._table_exists("api_credentials"):
                await conn.execute(
                    """
                    CREATE TABLE api_credentials (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        exchange TEXT NOT NULL UNIQUE,
                        api_key TEXT,
                        secret_key TEXT,
                        passphrase TEXT,
                        is_active BOOLEAN DEFAULT 1,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                logger.info("DB: created table api_credentials")

            # ---------------------------------
            # 9. Bot settings
            # ---------------------------------
            if not await self._table_exists("bot_settings"):
                await conn.execute(
                    """
                    CREATE TABLE bot_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        description TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                defaults = [
                    ("max_open_positions", "5", "Max concurrent arbitrages"),
                    ("default_leverage", "10", "Default leverage for all exchanges"),
                    ("max_monitored_pairs", "30", "Max monitored pairs"),
                ]
                for key, value, desc in defaults:
                    await conn.execute(
                        "INSERT OR IGNORE INTO bot_settings (key, value, description) VALUES (?, ?, ?)",
                        (key, value, desc)
                    )
                logger.info("DB: created table bot_settings with defaults")

            await conn.commit()
            logger.info(f"Database '{self.db_name}' initialized successfully")

        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise

    async def cleanup_old_data(self, days: int = 30) -> Dict[str, int]:
        """Cleanup old data from logs and events (async version)."""
        conn = await self._ensure_connection()
        deleted = {"events": 0, "metrics": 0, "emergency": 0}

        try:
            # Delete old events
            async with conn.execute(
                "DELETE FROM trade_events WHERE created_at < datetime('now', ?)",
                (f'-{days} days',)
            ) as cursor:
                deleted["events"] = cursor.rowcount

            # Delete old metrics
            async with conn.execute(
                "DELETE FROM system_metrics WHERE created_at < datetime('now', ?)",
                (f'-{days} days',)
            ) as cursor:
                deleted["metrics"] = cursor.rowcount

            # Delete resolved emergency positions older than 90 days
            async with conn.execute(
                "DELETE FROM emergency_positions WHERE status = 'resolved' AND resolved_at < datetime('now', '-90 days')"
            ) as cursor:
                deleted["emergency"] = cursor.rowcount

            await conn.commit()
            logger.info(f"DB cleanup: events={deleted['events']}, metrics={deleted['metrics']}, emergency={deleted['emergency']}")

        except Exception as e:
            logger.error(f"Cleanup error: {e}")

        return deleted

    async def get_table_stats(self) -> Dict[str, Optional[int]]:
        """Get table statistics (async version)."""
        tables = [
            "exchanges", "trading_pairs", "orders",
            "trade_events", "positions", "emergency_positions",
            "system_metrics", "api_credentials"
        ]
        stats = {}
        for table in tables:
            if await self._table_exists(table):
                count = await self._fetchval(f"SELECT COUNT(*) FROM {table}")
                stats[table] = int(count) if count else 0
            else:
                stats[table] = None
        return stats

    async def close(self):
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None
            self._initialized = False
