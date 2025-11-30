# market_engine.py
# OPTIMIZED v2.0 - O(N) search, event-driven cache
# Performance: O(N) instead of O(N²), uses bulk WsManager methods
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from loguru import logger
from config import DEAD_WS_TIMEOUT, TAKER_FEES, DEFAULT_TAKER_FEE

SPREAD_CACHE_TTL = 0.2  # FIX 6.2: было 0.5, ускорено для свежих данных
EARLY_EXIT_SPREAD_THRESHOLD = 1.0

@dataclass
class CachedSpread:
    data: Optional[Dict]
    timestamp: float
    def is_valid(self, ttl: float = SPREAD_CACHE_TTL) -> bool:
        return (time.time() - self.timestamp) < ttl

@dataclass
class MarketMetrics:
    spreads_calculated: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    opportunities_found: int = 0
    last_scan_duration_ms: float = 0.0
    last_scan_pairs_checked: int = 0
    fast_path_used: int = 0  # NEW: O(N) fast path counter
    
    def record_scan(self, duration_ms: float, pairs_checked: int):
        self.last_scan_duration_ms = duration_ms
        self.last_scan_pairs_checked = pairs_checked
    
    def to_dict(self) -> dict:
        cache_total = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / cache_total * 100) if cache_total > 0 else 0
        return {
            "spreads_calculated": self.spreads_calculated,
            "cache_hit_rate": f"{hit_rate:.1f}%",
            "opportunities_found": self.opportunities_found,
            "last_scan_ms": round(self.last_scan_duration_ms, 2),
            "fast_path_used": self.fast_path_used,
        }

class MarketEngine:
    """
    MarketEngine v2.0 - Optimized market logic layer.
    
    Key optimizations:
    1. O(N) search via get_best_prices() - finds best bid/ask across all exchanges in one pass
    2. Bulk orderbook fetch via get_all_books_for_symbol() - single call instead of N calls
    3. Event-driven cache invalidation via WsManager callbacks
    4. Tuple-based levels for compatibility with optimized WsManager
    """

    def __init__(self, ws_manager):
        self.ws = ws_manager
        self._spread_cache: Dict[Tuple[str, str, str], CachedSpread] = {}
        self.metrics = MarketMetrics()
        self._symbol_cache: Dict[str, Dict] = {}  # symbol -> {best_bid_ex, best_ask_ex, prices}
        self._cache_ts: Dict[str, float] = {}

    def _normalize_exchange(self, name: str) -> str:
        return (name or "").strip().lower()

    def _get_taker_fee(self, exchange: str) -> float:
        return float(TAKER_FEES.get(self._normalize_exchange(exchange), DEFAULT_TAKER_FEE))

    def _get_orderbook(self, exchange: str, symbol: str) -> Optional[Dict]:
        return self.ws.get_latest_book(self._normalize_exchange(exchange), symbol)

    def _is_book_fresh(self, ob: Dict) -> bool:
        if not ob: return False
        ts = ob.get("timestamp", 0)
        return isinstance(ts, (int, float)) and (time.time() - ts) <= DEAD_WS_TIMEOUT

    def _get_cached_spread(self, buy_ex: str, sell_ex: str, symbol: str) -> Optional[Dict]:
        key = (buy_ex, sell_ex, symbol)
        cached = self._spread_cache.get(key)
        if cached and cached.is_valid():
            self.metrics.cache_hits += 1
            return cached.data
        self.metrics.cache_misses += 1
        return None

    def _set_cached_spread(self, buy_ex: str, sell_ex: str, symbol: str, data: Optional[Dict]):
        self._spread_cache[(buy_ex, sell_ex, symbol)] = CachedSpread(data=data, timestamp=time.time())

    def clear_cache(self):
        self._spread_cache.clear()
        self._symbol_cache.clear()
        self._cache_ts.clear()

    def invalidate_symbol(self, symbol: str):
        """Invalidate cache for specific symbol (called on orderbook update)."""
        keys_to_remove = [k for k in self._spread_cache if k[2] == symbol]
        for k in keys_to_remove:
            del self._spread_cache[k]
        self._symbol_cache.pop(symbol, None)
        self._cache_ts.pop(symbol, None)

    # ============================================================
    # VWAP CALCULATION
    # ============================================================

    def calculate_vwap(self, levels, target_volume: float, min_fill_ratio: float = 0.98) -> Optional[float]:
        """Calculate VWAP with liquidity check. Supports both list and tuple levels."""
        if not levels or target_volume <= 0:
            return None
        total_cost = 0.0
        collected = 0.0
        for level in levels:
            try:
                price = float(level[0])
                qty = float(level[1])
            except (IndexError, TypeError, ValueError):
                continue
            if price <= 0 or qty <= 0:
                continue
            need = target_volume - collected
            if need <= 0:
                break
            take = min(qty, need)
            total_cost += take * price
            collected += take
            if collected >= target_volume:
                break
        if collected < target_volume * min_fill_ratio:
            return None
        return total_cost / collected

    def _get_side_price(self, orderbook: Dict, side: str, volume: float) -> Optional[float]:
        """VWAP for specific side of orderbook."""
        if not orderbook or volume <= 0:
            return None
        levels = orderbook.get("asks" if side == "buy" else "bids", [])
        return self.calculate_vwap(levels, volume) if levels else None

    # ============================================================
    # O(N) FAST PATH - Find best opportunity in single pass
    # ============================================================

    def find_best_opportunity_fast(self, symbol: str, volume_in_coin: float, min_spread_pct: float = 0.0) -> Optional[Dict]:
        """
        O(N) FAST PATH: Find best arbitrage opportunity.
        
        Instead of checking all N² pairs, we:
        1. Get best prices from all exchanges in ONE call
        2. Find exchange with highest bid (best to sell)
        3. Find exchange with lowest ask (best to buy)
        4. Calculate spread for this single pair
        
        Complexity: O(N) instead of O(N²)
        """
        start_time = time.perf_counter()
        
        # Get all best prices in ONE call (uses WsManager bulk method)
        prices = self.ws.get_best_prices(symbol)
        
        if not prices:
            return None
        
        # Find best bid (highest) and best ask (lowest)
        best_bid_ex = None
        best_bid_price = 0.0
        best_ask_ex = None
        best_ask_price = float('inf')
        
        for ex, (bid, ask) in prices.items():
            if bid is not None and bid > best_bid_price:
                best_bid_price = bid
                best_bid_ex = ex
            if ask is not None and ask < best_ask_price:
                best_ask_price = ask
                best_ask_ex = ex
        
        # Need both sides and different exchanges
        if not best_bid_ex or not best_ask_ex or best_bid_ex == best_ask_ex:
            return None
        
        # No arbitrage if ask >= bid
        if best_ask_price >= best_bid_price:
            return None
        
        # Calculate spread
        raw_spread_pct = (best_bid_price - best_ask_price) / best_ask_price * 100.0
        
        fee_buy = self._get_taker_fee(best_ask_ex)
        fee_sell = self._get_taker_fee(best_bid_ex)
        full_cycle_fees = (fee_buy + fee_sell) * 2
        net_full = raw_spread_pct - full_cycle_fees
        
        if net_full < min_spread_pct:
            return None
        
        self.metrics.fast_path_used += 1
        duration_ms = (time.perf_counter() - start_time) * 1000
        self.metrics.record_scan(duration_ms, len(prices))
        self.metrics.opportunities_found += 1
        
        return {
            "valid": True,
            "symbol": symbol,
            "volume": volume_in_coin,
            "buy_exchange": best_ask_ex,
            "sell_exchange": best_bid_ex,
            "buy_price": best_ask_price,
            "sell_price": best_bid_price,
            "raw_spread_pct": round(raw_spread_pct, 4),
            "net_entry_spread_pct": round(raw_spread_pct - fee_buy - fee_sell, 4),
            "net_full_spread_pct": round(net_full, 4),
            "fee_buy_pct": fee_buy,
            "fee_sell_pct": fee_sell,
            "spread_pct": round(net_full, 4),
            "timestamp": time.time(),
            "fast_path": True,
        }

    # ============================================================
    # DETAILED SPREAD CHECK (with VWAP)
    # ============================================================

    async def check_spread(self, symbol: str, buy_exchange: str, sell_exchange: str, 
                          volume_in_coin: float, use_cache: bool = True) -> Optional[Dict]:
        """Calculate spread between two exchanges with VWAP."""
        if volume_in_coin <= 0:
            return None
        
        be = self._normalize_exchange(buy_exchange)
        se = self._normalize_exchange(sell_exchange)
        if be == se:
            return None
        
        if use_cache:
            cached = self._get_cached_spread(be, se, symbol)
            if cached is not None:
                return cached
        
        ob_buy = self._get_orderbook(be, symbol)
        ob_sell = self._get_orderbook(se, symbol)
        
        if not ob_buy or not ob_sell:
            self._set_cached_spread(be, se, symbol, None)
            return None
        
        if not self._is_book_fresh(ob_buy) or not self._is_book_fresh(ob_sell):
            self._set_cached_spread(be, se, symbol, None)
            return None
        
        price_buy = self._get_side_price(ob_buy, "buy", volume_in_coin)
        price_sell = self._get_side_price(ob_sell, "sell", volume_in_coin)
        
        if price_buy is None or price_sell is None or price_sell < price_buy:
            self._set_cached_spread(be, se, symbol, None)
            return None
        
        raw_spread_pct = (price_sell - price_buy) / price_buy * 100.0
        fee_buy = self._get_taker_fee(be)
        fee_sell = self._get_taker_fee(se)
        entry_fees = fee_buy + fee_sell
        full_cycle_fees = entry_fees * 2
        
        result = {
            "valid": True,
            "symbol": symbol,
            "volume": volume_in_coin,
            "buy_exchange": be,
            "sell_exchange": se,
            "buy_price": price_buy,
            "sell_price": price_sell,
            "raw_spread_pct": round(raw_spread_pct, 4),
            "net_entry_spread_pct": round(raw_spread_pct - entry_fees, 4),
            "net_full_spread_pct": round(raw_spread_pct - full_cycle_fees, 4),
            "fee_buy_pct": fee_buy,
            "fee_sell_pct": fee_sell,
            "timestamp": min(ob_buy["timestamp"], ob_sell["timestamp"]),
            "spread_pct": round(raw_spread_pct - full_cycle_fees, 4),
        }
        
        self._set_cached_spread(be, se, symbol, result)
        self.metrics.spreads_calculated += 1
        return result

    # ============================================================
    # FULL SEARCH (with VWAP, for precise entry)
    # ============================================================

    async def find_best_opportunity(self, symbol: str, volume_in_coin: float,
                                   exchanges: Optional[List[str]] = None,
                                   min_spread_pct: float = 0.0,
                                   use_fast_path: bool = True) -> Optional[Dict]:
        """
        Find best arbitrage opportunity.
        
        If use_fast_path=True (default), uses O(N) algorithm with best bid/ask.
        For precise entry with VWAP, set use_fast_path=False.
        """
        if volume_in_coin <= 0:
            return None
        
        # Try fast path first (O(N))
        if use_fast_path:
            result = self.find_best_opportunity_fast(symbol, volume_in_coin, min_spread_pct)
            if result:
                return result
        
        # Fallback to full check with VWAP
        start_time = time.time()
        # FIX #12: Используем реально активные биржи вместо статического списка
        ex_list = [self._normalize_exchange(e) for e in exchanges] if exchanges else self.ws.get_active_exchanges()
        
        if len(ex_list) < 2:
            return None
        
        best: Optional[Dict] = None
        pairs_checked = 0
        
        for be in ex_list:
            for se in ex_list:
                if be == se:
                    continue
                pairs_checked += 1
                result = await self.check_spread(symbol, be, se, volume_in_coin)
                if result and result["net_full_spread_pct"] >= min_spread_pct:
                    if best is None or result["net_full_spread_pct"] > best["net_full_spread_pct"]:
                        best = result
                        if best["net_full_spread_pct"] >= EARLY_EXIT_SPREAD_THRESHOLD:
                            break
            if best and best["net_full_spread_pct"] >= EARLY_EXIT_SPREAD_THRESHOLD:
                break
        
        duration_ms = (time.time() - start_time) * 1000
        self.metrics.record_scan(duration_ms, pairs_checked)
        
        if best:
            self.metrics.opportunities_found += 1
            logger.info(f"[BEST {symbol}] {best['buy_exchange']}->{best['sell_exchange']} | "
                       f"net={best['net_full_spread_pct']}% | scan={duration_ms:.1f}ms")
        
        return best

    async def has_opportunity(self, symbol: str, volume_in_coin: float,
                             exchanges: Optional[List[str]] = None,
                             min_spread_pct: float = 0.0) -> bool:
        """Quick check if any arbitrage opportunity exists."""
        # Use fast path for quick check
        result = self.find_best_opportunity_fast(symbol, volume_in_coin, min_spread_pct)
        return result is not None

    # ============================================================
    # POSITION PRICES & PNL
    # ============================================================

    async def get_position_prices(self, symbol: str, long_exchange: str,
                                  short_exchange: str, volume_in_coin: float) -> Optional[Dict]:
        """Get current prices for position PnL calculation."""
        if volume_in_coin <= 0:
            return None
        
        le = self._normalize_exchange(long_exchange)
        se = self._normalize_exchange(short_exchange)
        if le == se:
            return None
        
        ob_long = self._get_orderbook(le, symbol)
        ob_short = self._get_orderbook(se, symbol)
        
        if not ob_long or not ob_short:
            return None
        if not self._is_book_fresh(ob_long) or not self._is_book_fresh(ob_short):
            return None
        
        long_exit = self._get_side_price(ob_long, "sell", volume_in_coin)
        short_exit = self._get_side_price(ob_short, "buy", volume_in_coin)
        
        if long_exit is None or short_exit is None:
            return None
        
        return {
            "valid": True,
            "symbol": symbol,
            "long_exchange": le,
            "short_exchange": se,
            "long_exit_price": long_exit,
            "short_exit_price": short_exit,
            "timestamp": min(ob_long["timestamp"], ob_short["timestamp"]),
        }

    async def estimate_position_pnl(self, symbol: str, long_exchange: str,
                                   short_exchange: str, volume_in_coin: float,
                                   avg_entry_long: float, avg_entry_short: float) -> Optional[Dict]:
        """Estimate current PnL for open position."""
        prices = await self.get_position_prices(symbol, long_exchange, short_exchange, volume_in_coin)
        if not prices or not prices["valid"]:
            return None
        
        long_exit = prices["long_exit_price"]
        short_exit = prices["short_exit_price"]
        
        pnl_long = (long_exit - avg_entry_long) * volume_in_coin
        pnl_short = (avg_entry_short - short_exit) * volume_in_coin
        total_pnl = pnl_long + pnl_short
        
        current_spread = (avg_entry_short - avg_entry_long) - (short_exit - long_exit)
        current_spread_pct = current_spread / avg_entry_long * 100 if avg_entry_long > 0 else 0
        
        return {
            "valid": True,
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "volume": volume_in_coin,
            "avg_entry_long": avg_entry_long,
            "avg_entry_short": avg_entry_short,
            "current_exit_long": long_exit,
            "current_exit_short": short_exit,
            "pnl_long": round(pnl_long, 4),
            "pnl_short": round(pnl_short, 4),
            "total_pnl": round(total_pnl, 4),
            "current_spread_pct": round(current_spread_pct, 4),
            "timestamp": prices["timestamp"],
        }

    # ============================================================
    # BULK OPERATIONS (new!)
    # ============================================================

    def get_all_spreads_fast(self, symbol: str) -> Dict[str, float]:
        """
        Get rough spread estimate for all exchange pairs quickly.
        Uses get_best_prices() for O(N) complexity.
        Returns: {"{buy_ex}->{sell_ex}": spread_pct, ...}
        """
        prices = self.ws.get_best_prices(symbol)
        if not prices:
            return {}
        
        # Build list of (exchange, bid, ask)
        valid = [(ex, bid, ask) for ex, (bid, ask) in prices.items() if bid and ask]
        if len(valid) < 2:
            return {}
        
        result = {}
        for buy_ex, _, buy_ask in valid:
            for sell_ex, sell_bid, _ in valid:
                if buy_ex == sell_ex:
                    continue
                if sell_bid > buy_ask:
                    raw_spread = (sell_bid - buy_ask) / buy_ask * 100
                    fees = (self._get_taker_fee(buy_ex) + self._get_taker_fee(sell_ex)) * 2
                    net_spread = raw_spread - fees
                    if net_spread > 0:
                        result[f"{buy_ex}->{sell_ex}"] = round(net_spread, 4)
        
        return result

    def get_metrics(self) -> dict:
        """Get MarketEngine metrics."""
        return self.metrics.to_dict()
