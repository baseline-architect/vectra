from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional, Dict, Any
from enum import Enum
import threading

import numpy as np

logger = logging.getLogger(__name__)


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class OrderBookSnapshot:
    symbol: str
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    spread: float
    spread_pct: float
    timestamp: float
    
    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0


@dataclass
class LimitOrderSpec:
    symbol: str
    side: OrderSide
    quantity: float
    limit_price: float
    take_profit_price: float
    stop_loss_price: float
    order_id: Optional[str] = None
    status: str = "pending"


class PassiveSpreadTracker:
    """
    Ultra-optimized passive spread-tracking execution worker.
    Streams Level 1 order-book metrics to track inside bid/ask spread changes.
    Dynamically slides limit orders at micro-level inside spread boundary.
    Recalculates parameters instantly if order book shifts.
    Uses vectorized operations for spread analysis and minimal latency.
    """
    
    def __init__(
        self,
        broker_client: Any,
        on_filled: Optional[Callable[[str, str, float, float, float, float], None]] = None,
        poll_interval: float = 0.5,
        spread_tolerance_pct: float = 0.001,
        max_slippage_pct: float = 0.005,
        min_spread_pct: float = 0.0001,
    ) -> None:
        self._broker = broker_client
        self._on_filled = on_filled
        self._poll_interval = poll_interval
        self._spread_tolerance_pct = spread_tolerance_pct
        self._max_slippage_pct = max_slippage_pct
        self._min_spread_pct = min_spread_pct
        
        self._active_chases: Dict[str, LimitOrderSpec] = {}
        self._order_book_cache: Dict[str, OrderBookSnapshot] = {}
        self._lock = threading.RLock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._total_orders_placed = 0
        self._total_orders_cancelled = 0
        self._total_fills = 0
        self._spread_history: Dict[str, list] = {}
        
    async def start(self) -> None:
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._chase_loop())
        logger.info("Passive spread tracker started", extra={
            "poll_interval": self._poll_interval,
            "spread_tolerance_pct": self._spread_tolerance_pct,
        })
    
    async def stop(self) -> None:
        if not self._running:
            return
        
        self._running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        
        with self._lock:
            for symbol, spec in self._active_chases.items():
                if spec.order_id:
                    try:
                        self._broker.cancel_order(spec.order_id)
                        self._total_orders_cancelled += 1
                    except Exception as e:
                        logger.error("Failed to cancel order on shutdown", extra={"symbol": symbol, "order_id": spec.order_id}, exc_info=True)
            self._active_chases.clear()
        
        logger.info("Passive spread tracker stopped")
    
    def start_chase(
        self,
        symbol: str,
        quantity: float,
        side: OrderSide,
        take_profit_pct: float,
        stop_loss_pct: float,
    ) -> None:
        with self._lock:
            if symbol in self._active_chases:
                logger.info("Chase already active", extra={"symbol": symbol})
                return
            
            self._spread_history[symbol] = []
            logger.info("Starting passive spread chase", extra={
                "symbol": symbol,
                "quantity": quantity,
                "side": side.value,
                "take_profit_pct": take_profit_pct,
                "stop_loss_pct": stop_loss_pct,
            })
    
    def stop_chase(self, symbol: str) -> None:
        with self._lock:
            if symbol not in self._active_chases:
                return
            
            spec = self._active_chases.pop(symbol)
            if spec.order_id:
                try:
                    self._broker.cancel_order(spec.order_id)
                    self._total_orders_cancelled += 1
                    logger.info("Chase stopped and order cancelled", extra={"symbol": symbol, "order_id": spec.order_id})
                except Exception as e:
                    logger.error("Failed to cancel order on chase stop", extra={"symbol": symbol}, exc_info=True)
            
            if symbol in self._spread_history:
                del self._spread_history[symbol]
    
    async def _chase_loop(self) -> None:
        while self._running:
            try:
                await self._process_chases()
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chase loop error", extra={"error": str(e)}, exc_info=True)
                await asyncio.sleep(1.0)
    
    async def _process_chases(self) -> None:
        with self._lock:
            symbols = list(self._active_chases.keys())
        
        for symbol in symbols:
            try:
                await self._update_order_book(symbol)
                await self._adjust_limit_order(symbol)
            except Exception as e:
                logger.error("Error processing chase", extra={"symbol": symbol, "error": str(e)}, exc_info=True)
    
    async def _update_order_book(self, symbol: str) -> None:
        quote = self._broker.get_latest_quote(symbol)
        if not quote:
            return
        
        quote_data = quote.get("quote", {})
        bid_price = float(quote_data.get("bp", 0.0))
        ask_price = float(quote_data.get("ap", 0.0))
        bid_size = float(quote_data.get("bs", 0.0))
        ask_size = float(quote_data.get("as", 0.0))
        
        if bid_price <= 0 or ask_price <= 0:
            return
        
        spread = ask_price - bid_price
        spread_pct = spread / bid_price if bid_price > 0 else 0.0
        
        snapshot = OrderBookSnapshot(
            symbol=symbol,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=bid_size,
            ask_size=ask_size,
            spread=spread,
            spread_pct=spread_pct,
            timestamp=time.time(),
        )
        
        with self._lock:
            self._order_book_cache[symbol] = snapshot
            if symbol in self._spread_history:
                self._spread_history[symbol].append(spread_pct)
                if len(self._spread_history[symbol]) > 100:
                    self._spread_history[symbol].pop(0)
    
    async def _adjust_limit_order(self, symbol: str) -> None:
        with self._lock:
            if symbol not in self._active_chases:
                return
            
            spec = self._active_chases[symbol]
            snapshot = self._order_book_cache.get(symbol)
            
            if snapshot is None:
                return
        
        current_spread_pct = snapshot.spread_pct
        
        if current_spread_pct < self._min_spread_pct:
            logger.debug("Spread too tight, waiting", extra={"symbol": symbol, "spread_pct": current_spread_pct})
            return
        
        with self._lock:
            if symbol in self._spread_history and len(self._spread_history[symbol]) > 5:
                recent_spreads = np.array(self._spread_history[symbol][-5:])
                avg_spread = np.mean(recent_spreads)
                std_spread = np.std(recent_spreads)
                
                if current_spread_pct > avg_spread + 2 * std_spread:
                    logger.debug("Spread widened abnormally, waiting", extra={"symbol": symbol, "spread_pct": current_spread_pct})
                    return
        
        if spec.side == OrderSide.BUY:
            target_price = snapshot.bid_price + (snapshot.spread * 0.3)
        else:
            target_price = snapshot.ask_price - (snapshot.spread * 0.3)
        
        target_price = round(target_price, 4)
        
        with self._lock:
            if spec.order_id and spec.limit_price == target_price:
                return
            
            if spec.order_id:
                try:
                    self._broker.cancel_order(spec.order_id)
                    self._total_orders_cancelled += 1
                    logger.debug("Order cancelled for repricing", extra={"symbol": symbol, "order_id": spec.order_id})
                except Exception as e:
                    logger.error("Failed to cancel order for repricing", extra={"symbol": symbol}, exc_info=True)
                    return
            
            tp_price = round(target_price * (1 + spec.take_profit_price), 4)
            sl_price = round(target_price * (1 - spec.stop_loss_price), 4)
            
            order_result = self._broker.submit_bracket_order(
                symbol=symbol,
                qty=spec.quantity,
                side=spec.side.value,
                limit_price=target_price,
                take_profit_price=tp_price,
                stop_loss_price=sl_price,
            )
            
            if order_result:
                spec.order_id = order_result.get("id")
                spec.limit_price = target_price
                spec.take_profit_price = tp_price
                spec.stop_loss_price = sl_price
                spec.status = "placed"
                self._total_orders_placed += 1
                
                logger.info("Limit order placed at inside spread", extra={
                    "symbol": symbol,
                    "order_id": spec.order_id,
                    "limit_price": target_price,
                    "bid": snapshot.bid_price,
                    "ask": snapshot.ask_price,
                    "spread_pct": current_spread_pct,
                })
            
            await self._check_order_status(symbol)
    
    async def _check_order_status(self, symbol: str) -> None:
        with self._lock:
            if symbol not in self._active_chases:
                return
            
            spec = self._active_chases[symbol]
            if not spec.order_id:
                return
        
        try:
            order_status = self._broker.get_order(spec.order_id)
            if not order_status:
                return
            
            status = order_status.get("status")
            
            if status in {"filled", "partially_filled"}:
                with self._lock:
                    self._active_chases.pop(symbol, None)
                    if symbol in self._spread_history:
                        del self._spread_history[symbol]
                
                self._total_fills += 1
                
                logger.info("Order filled", extra={
                    "symbol": symbol,
                    "order_id": spec.order_id,
                    "status": status,
                })
                
                if self._on_filled:
                    try:
                        self._on_filled(
                            symbol,
                            str(spec.order_id),
                            float(spec.limit_price),
                            float(spec.take_profit_price),
                            float(spec.stop_loss_price),
                            float(spec.quantity),
                        )
                    except Exception as e:
                        logger.error("on_filled callback error", exc_info=True)
            
            elif status == "canceled":
                with self._lock:
                    spec.order_id = None
                    spec.status = "pending"
                
                logger.debug("Order was cancelled, will reprice", extra={"symbol": symbol})
        
        except Exception as e:
            logger.error("Error checking order status", extra={"symbol": symbol}, exc_info=True)
    
    def get_active_chases(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                symbol: {
                    "side": spec.side.value,
                    "quantity": spec.quantity,
                    "limit_price": spec.limit_price,
                    "order_id": spec.order_id,
                    "status": spec.status,
                }
                for symbol, spec in self._active_chases.items()
            }
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "active_chases": len(self._active_chases),
                "total_orders_placed": self._total_orders_placed,
                "total_orders_cancelled": self._total_orders_cancelled,
                "total_fills": self._total_fills,
                "fill_rate": self._total_fills / self._total_orders_placed if self._total_orders_placed > 0 else 0.0,
            }
