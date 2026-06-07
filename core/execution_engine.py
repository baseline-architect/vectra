from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional, List
from enum import Enum

import numpy as np
import pandas as pd

from core.memory_layer import HybridMemoryLayer
from core.vectorized_indicators import VectorizedIndicators
from core.rate_limiter import AlpacaRateLimiter
from core.async_websocket import DualPlaneWebSocketManager, WebSocketMessage
from core.spread_tracker import PassiveSpreadTracker, OrderSide
from core.circuit_breakers import CircuitBreakerManager, CircuitBreakerConfig, SafetyLevel
from core.secure_config import SecureConfig, ConfigManager, EnvironmentMode

from models.database import init_engine, create_all
from models.position import Position
from models.metrics import StrategyMetric
from models.repository import get_all_positions, get_position, upsert_position, delete_position

from execution.broker import BrokerClient
from strategy.data import bars_json_to_df, resample_weekly
from strategy.volatility import VolatilityRegimeFilter
from strategy.multi_timeframe import validate_golden_cross
from strategy.correlation import correlation_protector
from strategy.macro_gatekeeper import parse_events, should_block
from strategy.kelly import compute_kelly_for_symbol

from config.logging_config import configure_logging

logger = logging.getLogger(__name__)


class EngineState(Enum):
    INITIALIZING = "initializing"
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    SHUTDOWN = "shutdown"
    ERROR = "error"


@dataclass
class PositionSpec:
    symbol: str
    qty: float
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class UltraHardenedExecutionEngine:
    """
    Ultra-hardened, low-latency autonomous institutional execution engine.
    Integrates all hardened components for maximum performance and fault tolerance.
    """
    
    def __init__(self, config: SecureConfig) -> None:
        self._config = config
        self._state = EngineState.INITIALIZING
        self._lock = __import__("threading").RLock()
        
        self._memory_layer = HybridMemoryLayer(
            cache_max_size=config.cache_max_size,
            db_batch_size=config.db_batch_size,
            db_flush_interval=config.db_flush_interval,
        )
        
        self._rate_limiter = AlpacaRateLimiter()
        self._circuit_breaker = CircuitBreakerManager(
            config=CircuitBreakerConfig(
                max_daily_drawdown_pct=config.max_daily_drawdown_pct,
                max_consecutive_trades=config.max_consecutive_trades,
                max_trades_per_minute=config.max_trades_per_minute,
                max_price_change_pct=config.max_price_change_pct,
                max_position_value_pct=config.max_position_value_pct,
                cooldown_seconds=config.circuit_breaker_cooldown_seconds,
            )
        )
        
        self._broker = BrokerClient(config)
        self._indicators = VectorizedIndicators()
        
        self._websocket: Optional[DualPlaneWebSocketManager] = None
        self._spread_tracker: Optional[PassiveSpreadTracker] = None
        
        self._last_stream_event_ts: Optional[float] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        
        self._initial_equity: Optional[float] = None
        self._current_equity: Optional[float] = None
        
        logger.info("Ultra-hardened execution engine initialized", extra={
            "environment_mode": config.environment_mode.value,
            "tracked_symbols": config.tracked_symbols,
        })
    
    async def bootstrap(self) -> None:
        """Initialize all subsystems and prepare for trading."""
        with self._lock:
            if self._state != EngineState.INITIALIZING:
                raise RuntimeError(f"Engine cannot bootstrap from state {self._state.value}")
        
        configure_logging(self._config.log_level.value)
        
        init_engine(self._config.database_url)
        create_all()
        
        self._memory_layer.initialize()
        
        self._spread_tracker = PassiveSpreadTracker(
            broker_client=self._broker,
            on_filled=self._on_order_filled,
            poll_interval=0.5,
            spread_tolerance_pct=self._config.spread_tolerance_pct,
            max_slippage_pct=self._config.max_slippage_pct,
        )
        
        await self._spread_tracker.start()
        
        await self._initialize_websocket()
        
        initial_equity = await self._fetch_account_equity()
        if initial_equity:
            self._initial_equity = initial_equity
            self._current_equity = initial_equity
            self._circuit_breaker.set_initial_equity(initial_equity)
        
        await self._rehydrate_positions()
        
        with self._lock:
            self._state = EngineState.READY
        
        logger.info("Engine bootstrapped successfully", extra={
            "initial_equity": self._initial_equity,
            "tracked_symbols": self._config.tracked_symbols,
        })
    
    async def _initialize_websocket(self) -> None:
        """Initialize dual-plane WebSocket manager."""
        api_key = self._config.alpaca_api_key.get_secret_value()
        api_secret = self._config.alpaca_api_secret.get_secret_value()
        
        self._websocket = DualPlaneWebSocketManager(
            primary_url=self._config.alpaca_stream_url,
            api_key=api_key,
            api_secret=api_secret,
            on_message=self._on_websocket_message,
            symbols=self._config.tracked_symbols,
            polling_interval=5.0,
        )
        
        await self._websocket.start()
        logger.info("WebSocket manager initialized")
    
    async def _fetch_account_equity(self) -> Optional[float]:
        """Fetch current account equity with rate limiting."""
        if not self._rate_limiter.consume_account(block=True, timeout=5.0):
            logger.warning("Rate limited while fetching account equity")
            return None
        
        account = self._broker.get_account()
        if account:
            equity = float(account.get("equity", 0.0) or account.get("cash", 0.0) or 0.0)
            if equity == 0.0 and self._config.environment_mode == EnvironmentMode.SHADOW:
                equity = 100000.0
            return equity
        return None
    
    async def rehydrate_positions(self) -> List[str]:
        """Rehydrate active positions from database."""
        return await self._rehydrate_positions()
    
    async def _rehydrate_positions(self) -> List[str]:
        """Internal position rehydration."""
        symbols: List[str] = []
        
        try:
            from models.database import session_scope
            with session_scope() as s:
                positions = get_all_positions(s)
                for p in positions:
                    logger.info("Rehydrated position", extra={
                        "symbol": p.symbol,
                        "qty": p.qty,
                        "entry_price": p.entry_price,
                        "stop_loss": p.stop_loss,
                        "take_profit": p.take_profit,
                    })
                    
                    self._memory_layer.upsert_position(
                        symbol=p.symbol,
                        qty=p.qty,
                        entry_price=p.entry_price,
                        stop_loss=p.stop_loss,
                        take_profit=p.take_profit,
                    )
                    
                    symbols.append(p.symbol)
        except Exception as e:
            logger.error("Failed to rehydrate positions", exc_info=True)
        
        return symbols
    
    async def start(self) -> None:
        """Start the main execution loop."""
        with self._lock:
            if self._state != EngineState.READY:
                raise RuntimeError(f"Engine cannot start from state {self._state.value}")
            self._state = EngineState.RUNNING
            self._running = True
        
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        logger.info("Engine started", extra={"state": self._state.value})
    
    async def stop(self) -> None:
        """Gracefully shutdown the engine."""
        with self._lock:
            self._state = EngineState.SHUTDOWN
            self._running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._spread_tracker:
            await self._spread_tracker.stop()
        
        if self._websocket:
            await self._websocket.stop()
        
        self._memory_layer.shutdown()
        
        logger.info("Engine stopped")
    
    async def _heartbeat_loop(self) -> None:
        """Periodic heartbeat and health checks."""
        while self._running:
            try:
                await asyncio.sleep(self._config.heartbeat_interval_seconds)
                
                current_time = time.time()
                if self._last_stream_event_ts:
                    stream_latency = current_time - self._last_stream_event_ts
                    if not self._circuit_breaker.record_latency(stream_latency * 1000):
                        logger.warning("Latency circuit breaker triggered")
                
                await self._update_equity()
                
                if not self._circuit_breaker.is_trading_allowed():
                    logger.warning("Trading blocked by circuit breakers", extra={
                        "safety_level": self._circuit_breaker.get_global_safety_level().value,
                    })
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Heartbeat loop error", exc_info=True)
    
    async def _update_equity(self) -> None:
        """Update current equity and check drawdown."""
        equity = await self._fetch_account_equity()
        if equity:
            self._current_equity = equity
            if not self._circuit_breaker.update_equity(equity):
                logger.critical("Daily drawdown circuit breaker triggered")
    
    def _on_websocket_message(self, message: WebSocketMessage) -> None:
        """Handle incoming WebSocket messages."""
        self._last_stream_event_ts = time.time()
        
        if message.event_type == "bar":
            asyncio.create_task(self._process_bar_event(message))
    
    async def _process_bar_event(self, message: WebSocketMessage) -> None:
        """Process bar event with full filter evaluation."""
        start_time = time.time()
        
        try:
            symbol = message.symbol
            if not symbol:
                return
            
            if not self._circuit_breaker.is_trading_allowed():
                logger.debug("Trading blocked by circuit breakers", extra={"symbol": symbol})
                return
            
            filters = await self._evaluate_pretrade_filters(symbol)
            
            logger.info("Stream event filters", extra={"symbol": symbol, **filters})
            
            if self._entry_condition(filters):
                await self._execute_entry(symbol, filters)
            
            latency_ms = (time.time() - start_time) * 1000
            self._circuit_breaker.record_latency(latency_ms)
            
        except Exception as e:
            logger.error("Error processing bar event", exc_info=True)
    
    async def _evaluate_pretrade_filters(self, symbol: str) -> Dict[str, Any]:
        """Evaluate all pre-trade filters with vectorized indicators."""
        now = datetime.now(timezone.utc)
        
        daily_limit = max(self._config.sma_slow * 3, 260)
        weekly_limit = max(self._config.ema_weeks * 3, 60)
        
        if not self._rate_limiter.consume_data(block=True, timeout=5.0):
            logger.warning("Rate limited while fetching bars")
            return {"allowed": False, "reason": "rate_limited"}
        
        daily_data = self._broker.get_bars(symbol, timeframe="1Day", limit=daily_limit)
        weekly_data = self._broker.get_bars(symbol, timeframe="1Week", limit=weekly_limit)
        
        daily_df = bars_json_to_df(daily_data or {})
        weekly_df = bars_json_to_df(weekly_data or {})
        
        if weekly_df.empty and not daily_df.empty:
            weekly_df = resample_weekly(daily_df)
        
        reasons: Dict[str, Any] = {}
        allowed = True
        
        if self._config.enable_volatility_filter:
            vstate = self._evaluate_volatility_filter(daily_df)
            reasons["volatility_state"] = vstate
            if vstate["high_volatility"]:
                allowed = False
        
        if self._config.enable_mtf_validation:
            gcv = self._evaluate_mtf_validation(daily_df, weekly_df)
            reasons["golden_cross_validation"] = gcv
            if gcv["golden_cross"] and not gcv["valid"]:
                allowed = False
        
        if self._config.enable_correlation_protector:
            cres = await self._evaluate_correlation_protector(symbol)
            reasons["correlation"] = cres
            if symbol not in cres.get("allowed_symbols", []):
                allowed = False
        
        if self._config.enable_macro_gatekeeper:
            mres = self._evaluate_macro_gatekeeper(now, symbol)
            reasons["macro_gatekeeper"] = mres
            if mres["blocked"]:
                allowed = False
        
        reasons["allowed"] = allowed
        return reasons
    
    def _evaluate_volatility_filter(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Evaluate volatility filter using vectorized indicators."""
        if df.empty or len(df) < self._config.atr_window + 1:
            return {
                "current_atr": 0.0,
                "atr_p90": 0.0,
                "current_std": 0.0,
                "std_p90": 0.0,
                "high_volatility": False,
            }
        
        high = df["high"].values.astype(np.float64)
        low = df["low"].values.astype(np.float64)
        close = df["close"].values.astype(np.float64)
        
        atr_series = self._indicators.atr(high, low, close, self._config.atr_window)
        std_series = self._indicators.rolling_std(close, self._config.std_window)
        
        valid_atr = atr_series[~np.isnan(atr_series)]
        valid_std = std_series[~np.isnan(std_series)]
        
        if len(valid_atr) == 0 or len(valid_std) == 0:
            return {
                "current_atr": 0.0,
                "atr_p90": 0.0,
                "current_std": 0.0,
                "std_p90": 0.0,
                "high_volatility": False,
            }
        
        p90_atr = np.percentile(valid_atr, self._config.volatility_percentile * 100)
        p90_std = np.percentile(valid_std, self._config.volatility_percentile * 100)
        
        current_atr = float(valid_atr[-1])
        current_std = float(valid_std[-1])
        
        return {
            "current_atr": current_atr,
            "atr_p90": float(p90_atr),
            "current_std": current_std,
            "std_p90": float(p90_std),
            "high_volatility": current_atr >= p90_atr or current_std >= p90_std,
        }
    
    def _evaluate_mtf_validation(self, daily_df: pd.DataFrame, weekly_df: pd.DataFrame) -> Dict[str, Any]:
        """Evaluate multi-timeframe validation using vectorized indicators."""
        if daily_df.empty:
            return {
                "golden_cross": False,
                "price_above_21w_ema": False,
                "valid": False,
                "last_price": 0.0,
                "last_21w_ema": 0.0,
            }
        
        close = daily_df["close"].values.astype(np.float64)
        
        sma_fast = self._indicators.sma(close, self._config.sma_fast)
        sma_slow = self._indicators.sma(close, self._config.sma_slow)
        
        if len(sma_fast) < 2 or len(sma_slow) < 2:
            return {
                "golden_cross": False,
                "price_above_21w_ema": False,
                "valid": False,
                "last_price": float(close[-1]),
                "last_21w_ema": 0.0,
            }
        
        golden_cross = sma_fast[-1] > sma_slow[-1] and sma_fast[-2] <= sma_slow[-2]
        
        price_above_21w_ema = False
        last_21w_ema = 0.0
        
        if not weekly_df.empty:
            weekly_close = weekly_df["close"].values.astype(np.float64)
            ema_21w = self._indicators.ema(weekly_close, self._config.ema_weeks)
            if len(ema_21w) > 0:
                last_21w_ema = float(ema_21w[-1])
                price_above_21w_ema = float(close[-1]) > last_21w_ema
        
        valid = not (golden_cross and not price_above_21w_ema)
        
        return {
            "golden_cross": golden_cross,
            "price_above_21w_ema": price_above_21w_ema,
            "valid": valid,
            "last_price": float(close[-1]),
            "last_21w_ema": last_21w_ema,
        }
    
    async def _evaluate_correlation_protector(self, symbol: str) -> Dict[str, Any]:
        """Evaluate correlation protector with vectorized operations."""
        try:
            from models.database import session_scope
            with session_scope() as s:
                prices = {}
                for sym in self._config.tracked_symbols:
                    data = self._broker.get_bars(sym, timeframe="1Day", limit=self._config.corr_lookback_days * 3)
                    df = bars_json_to_df(data or {})
                    if not df.empty:
                        prices[sym] = df["close"].values.astype(np.float64)
                
                if not prices:
                    return {"threshold": self._config.corr_threshold, "allowed_symbols": self._config.tracked_symbols, "leaders": {}}
                
                prices_array = np.column_stack([prices[sym] for sym in self._config.tracked_symbols])
                corr_matrix = np.corrcoef(prices_array.T)
                
                allowed_symbols = set(self._config.tracked_symbols)
                leaders = {}
                
                for i, sym1 in enumerate(self._config.tracked_symbols):
                    for j, sym2 in enumerate(self._config.tracked_symbols):
                        if i < j and corr_matrix[i, j] > self._config.corr_threshold:
                            momentum1 = self._calculate_momentum(prices[sym1])
                            momentum2 = self._calculate_momentum(prices[sym2])
                            if momentum1 > momentum2:
                                leaders[sym2] = sym1
                                if sym2 in allowed_symbols:
                                    allowed_symbols.remove(sym2)
                            else:
                                leaders[sym1] = sym2
                                if sym1 in allowed_symbols:
                                    allowed_symbols.remove(sym1)
                
                return {
                    "threshold": self._config.corr_threshold,
                    "allowed_symbols": sorted(allowed_symbols),
                    "leaders": leaders,
                }
        except Exception as e:
            logger.error("Error evaluating correlation protector", exc_info=True)
            return {"threshold": self._config.corr_threshold, "allowed_symbols": self._config.tracked_symbols, "leaders": {}}
    
    def _calculate_momentum(self, prices: np.ndarray) -> float:
        """Calculate momentum using vectorized operations."""
        if len(prices) < self._config.momentum_lookback_days:
            return 0.0
        return float((prices[-1] - prices[-self._config.momentum_lookback_days]) / prices[-self._config.momentum_lookback_days])
    
    def _evaluate_macro_gatekeeper(self, now: datetime, symbol: str) -> Dict[str, Any]:
        """Evaluate macro gatekeeper."""
        events = parse_events(self._config.macro_events)
        block, blockers = should_block(now, symbol, events, horizon_hours=self._config.macro_block_hours)
        
        return {
            "blocked": block,
            "blockers": [
                {"type": b.type, "symbol": b.symbol, "start": b.start.isoformat(), "impact": b.impact}
                for b in blockers
            ],
        }
    
    def _entry_condition(self, filters: Dict[str, Any]) -> bool:
        """Determine if entry conditions are met."""
        gcv = filters.get("golden_cross_validation", {})
        return bool(gcv.get("golden_cross") and filters.get("allowed"))
    
    async def _execute_entry(self, symbol: str, filters: Dict[str, Any]) -> None:
        """Execute entry order with all safety checks."""
        if not self._rate_limiter.consume_order(block=True, timeout=5.0):
            logger.warning("Rate limited while executing entry", extra={"symbol": symbol})
            return
        
        quote = self._broker.get_latest_quote(symbol)
        if not quote:
            return
        
        quote_data = quote.get("quote", {})
        bid_price = float(quote_data.get("bp", 0.0))
        
        if bid_price <= 0:
            return
        
        qty = await self._calculate_position_size(symbol, bid_price)
        if qty <= 0:
            return
        
        if not self._circuit_breaker.record_trade(symbol, bid_price, qty):
            logger.warning("Trade blocked by circuit breaker", extra={"symbol": symbol})
            return
        
        self._spread_tracker.start_chase(
            symbol=symbol,
            quantity=qty,
            side=OrderSide.BUY,
            take_profit_pct=self._config.take_profit_pct,
            stop_loss_pct=self._config.stop_loss_pct,
        )
        
        logger.info("Entry signal executed", extra={
            "symbol": symbol,
            "qty": qty,
            "bid_price": bid_price,
        })
    
    async def _calculate_position_size(self, symbol: str, price: float) -> int:
        """Calculate position size using Kelly criterion."""
        try:
            from models.database import session_scope
            with session_scope() as s:
                kr = compute_kelly_for_symbol(s, symbol)
            
            equity = self._current_equity or 100000.0
            risk_capital = max(0.0, kr.kelly_fraction) * equity
            
            if price <= 0:
                return 0
            
            qty = int(risk_capital // price)
            return max(qty, 0)
        except Exception as e:
            logger.error("Error calculating position size", exc_info=True)
            return 0
    
    def _on_order_filled(self, symbol: str, order_id: str, limit_price: float, tp_price: float, sl_price: float, qty: float) -> None:
        """Handle order fill event."""
        self._memory_layer.upsert_position(
            symbol=symbol,
            qty=qty,
            entry_price=limit_price,
            stop_loss=sl_price,
            take_profit=tp_price,
        )
        
        logger.info("Order filled", extra={
            "symbol": symbol,
            "order_id": order_id,
            "limit_price": limit_price,
            "take_profit_price": tp_price,
            "stop_loss_price": sl_price,
            "qty": qty,
        })
    
    def get_state(self) -> EngineState:
        with self._lock:
            return self._state
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self._state.value,
            "memory_layer": self._memory_layer.stats(),
            "rate_limiter": self._rate_limiter.stats(),
            "circuit_breaker": self._circuit_breaker.get_stats(),
            "spread_tracker": self._spread_tracker.get_stats() if self._spread_tracker else {},
            "websocket": self._websocket.get_stats() if self._websocket else {},
            "initial_equity": self._initial_equity,
            "current_equity": self._current_equity,
        }
