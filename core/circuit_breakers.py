from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Dict, Any, Callable, List
from collections import deque

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class SafetyLevel(Enum):
    NORMAL = "normal"
    CAUTION = "caution"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class CircuitBreakerConfig:
    max_daily_drawdown_pct: float = 0.15
    max_consecutive_trades: int = 20
    max_trades_per_minute: int = 5
    max_price_change_pct: float = 0.20
    max_position_value_pct: float = 0.30
    max_order_rejection_rate: float = 0.10
    max_latency_ms: float = 500.0
    cooldown_seconds: float = 300.0
    warning_threshold_pct: float = 0.70


@dataclass
class CircuitBreakerEvent:
    breaker_name: str
    triggered_at: datetime
    safety_level: SafetyLevel
    reason: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    resolved_at: Optional[datetime] = None


class CircuitBreaker:
    """
    Individual circuit breaker with configurable thresholds and cooldown logic.
    Automatically transitions between CLOSED, OPEN, and HALF_OPEN states.
    """
    
    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig,
        on_trigger: Optional[Callable[[CircuitBreakerEvent], None]] = None,
    ) -> None:
        self._name = name
        self._config = config
        self._on_trigger = on_trigger
        self._state = CircuitBreakerState.CLOSED
        self._triggered_at: Optional[datetime] = None
        self._violation_count = 0
        self._last_violation: Optional[datetime] = None
        self._lock = threading.RLock()
        self._events: List[CircuitBreakerEvent] = []
        
    def check(self, value: float, threshold: float, context: Optional[Dict[str, Any]] = None) -> bool:
        with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitBreakerState.HALF_OPEN
                    logger.info("Circuit breaker entering half-open state", extra={"breaker": self._name})
                else:
                    return False
            
            if value > threshold:
                self._violation_count += 1
                self._last_violation = datetime.now(timezone.utc)
                
                warning_threshold = threshold * self._config.warning_threshold_pct
                
                if value > threshold:
                    safety_level = SafetyLevel.EMERGENCY
                elif value > warning_threshold:
                    safety_level = SafetyLevel.CRITICAL
                else:
                    safety_level = SafetyLevel.CAUTION
                
                if value > threshold:
                    self._trigger(safety_level, f"Threshold exceeded: {value} > {threshold}", context or {})
                    return False
            
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
                self._violation_count = 0
                logger.info("Circuit breaker reset to closed state", extra={"breaker": self._name})
            
            return True
    
    def _should_attempt_reset(self) -> bool:
        if self._triggered_at is None:
            return True
        
        elapsed = (datetime.now(timezone.utc) - self._triggered_at).total_seconds()
        return elapsed >= self._config.cooldown_seconds
    
    def _trigger(self, safety_level: SafetyLevel, reason: str, metadata: Dict[str, Any]) -> None:
        self._state = CircuitBreakerState.OPEN
        self._triggered_at = datetime.now(timezone.utc)
        
        event = CircuitBreakerEvent(
            breaker_name=self._name,
            triggered_at=self._triggered_at,
            safety_level=safety_level,
            reason=reason,
            metadata=metadata,
        )
        self._events.append(event)
        
        logger.critical("Circuit breaker triggered", extra={
            "breaker": self._name,
            "safety_level": safety_level.value,
            "reason": reason,
            "metadata": metadata,
        })
        
        if self._on_trigger:
            try:
                self._on_trigger(event)
            except Exception as e:
                logger.error("Circuit breaker callback error", exc_info=True)
    
    def reset(self) -> None:
        with self._lock:
            self._state = CircuitBreakerState.CLOSED
            self._triggered_at = None
            self._violation_count = 0
            self._last_violation = None
            logger.info("Circuit breaker manually reset", extra={"breaker": self._name})
    
    def get_state(self) -> CircuitBreakerState:
        with self._lock:
            return self._state
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self._name,
                "state": self._state.value,
                "violation_count": self._violation_count,
                "last_violation": self._last_violation.isoformat() if self._last_violation else None,
                "triggered_at": self._triggered_at.isoformat() if self._triggered_at else None,
                "total_events": len(self._events),
            }


class CircuitBreakerManager:
    """
    Centralized circuit breaker manager with multiple safety mechanisms.
    Enforces hard-coded execution circuit breakers for capital protection.
    """
    
    def __init__(self, config: Optional[CircuitBreakerConfig] = None) -> None:
        self._config = config or CircuitBreakerConfig()
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        self._global_safety_level = SafetyLevel.NORMAL
        self._emergency_shutdown = False
        self._trade_history: deque = deque(maxlen=1000)
        self._price_history: Dict[str, deque] = {}
        self._initial_equity: Optional[float] = None
        self._current_equity: Optional[float] = None
        self._position_values: Dict[str, float] = {}
        self._order_rejections = 0
        self._order_attempts = 0
        self._latency_samples: deque = deque(maxlen=100)
        
        self._initialize_breakers()
    
    def _initialize_breakers(self) -> None:
        self._breakers["daily_drawdown"] = CircuitBreaker(
            name="daily_drawdown",
            config=self._config,
            on_trigger=self._on_breaker_trigger,
        )
        
        self._breakers["consecutive_trades"] = CircuitBreaker(
            name="consecutive_trades",
            config=self._config,
            on_trigger=self._on_breaker_trigger,
        )
        
        self._breakers["trade_rate"] = CircuitBreaker(
            name="trade_rate",
            config=self._config,
            on_trigger=self._on_breaker_trigger,
        )
        
        self._breakers["price_outlier"] = CircuitBreaker(
            name="price_outlier",
            config=self._config,
            on_trigger=self._on_breaker_trigger,
        )
        
        self._breakers["position_limit"] = CircuitBreaker(
            name="position_limit",
            config=self._config,
            on_trigger=self._on_breaker_trigger,
        )
        
        self._breakers["order_rejection"] = CircuitBreaker(
            name="order_rejection",
            config=self._config,
            on_trigger=self._on_breaker_trigger,
        )
        
        self._breakers["latency"] = CircuitBreaker(
            name="latency",
            config=self._config,
            on_trigger=self._on_breaker_trigger,
        )
        
        logger.info("Circuit breakers initialized", extra={"breakers": list(self._breakers.keys())})
    
    def _on_breaker_trigger(self, event: CircuitBreakerEvent) -> None:
        if event.safety_level in {SafetyLevel.CRITICAL, SafetyLevel.EMERGENCY}:
            self._global_safety_level = event.safety_level
            if event.safety_level == SafetyLevel.EMERGENCY:
                self._emergency_shutdown = True
                logger.critical("EMERGENCY SHUTDOWN TRIGGERED", extra={"breaker": event.breaker_name})
    
    def set_initial_equity(self, equity: float) -> None:
        with self._lock:
            self._initial_equity = equity
            self._current_equity = equity
            logger.info("Initial equity set", extra={"equity": equity})
    
    def update_equity(self, equity: float) -> bool:
        with self._lock:
            if self._initial_equity is None:
                self._initial_equity = equity
            
            self._current_equity = equity
            drawdown_pct = (self._initial_equity - equity) / self._initial_equity if self._initial_equity > 0 else 0.0
            
            return self._breakers["daily_drawdown"].check(
                value=drawdown_pct,
                threshold=self._config.max_daily_drawdown_pct,
                context={"current_equity": equity, "drawdown_pct": drawdown_pct},
            )
    
    def record_trade(self, symbol: str, price: float, qty: float) -> bool:
        with self._lock:
            timestamp = time.time()
            self._trade_history.append({"symbol": symbol, "price": price, "qty": qty, "timestamp": timestamp})
            
            if symbol not in self._price_history:
                self._price_history[symbol] = deque(maxlen=10)
            self._price_history[symbol].append(price)
            
            consecutive_count = self._count_consecutive_trades()
            if not self._breakers["consecutive_trades"].check(
                value=consecutive_count,
                threshold=self._config.max_consecutive_trades,
                context={"symbol": symbol, "consecutive_count": consecutive_count},
            ):
                return False
            
            trades_per_minute = self._count_trades_per_minute()
            if not self._breakers["trade_rate"].check(
                value=trades_per_minute,
                threshold=self._config.max_trades_per_minute,
                context={"symbol": symbol, "trades_per_minute": trades_per_minute},
            ):
                return False
            
            if not self._check_price_outlier(symbol, price):
                return False
            
            return True
    
    def _count_consecutive_trades(self) -> int:
        if not self._trade_history:
            return 0
        
        count = 0
        now = time.time()
        for trade in reversed(self._trade_history):
            if now - trade["timestamp"] < 60.0:
                count += 1
            else:
                break
        return count
    
    def _count_trades_per_minute(self) -> int:
        if not self._trade_history:
            return 0
        
        count = 0
        now = time.time()
        for trade in self._trade_history:
            if now - trade["timestamp"] < 60.0:
                count += 1
        return count
    
    def _check_price_outlier(self, symbol: str, price: float) -> bool:
        if symbol not in self._price_history or len(self._price_history[symbol]) < 2:
            return True
        
        prices = list(self._price_history[symbol])
        prev_price = prices[-2]
        
        if prev_price == 0:
            return True
        
        change_pct = abs(price - prev_price) / prev_price
        
        return self._breakers["price_outlier"].check(
            value=change_pct,
            threshold=self._config.max_price_change_pct,
            context={"symbol": symbol, "prev_price": prev_price, "current_price": price, "change_pct": change_pct},
        )
    
    def update_position_value(self, symbol: str, value: float) -> bool:
        with self._lock:
            self._position_values[symbol] = value
            
            if self._current_equity is None or self._current_equity == 0:
                return True
            
            total_position_value = sum(self._position_values.values())
            position_pct = total_position_value / self._current_equity
            
            return self._breakers["position_limit"].check(
                value=position_pct,
                threshold=self._config.max_position_value_pct,
                context={"symbol": symbol, "total_position_value": total_position_value, "position_pct": position_pct},
            )
    
    def record_order_attempt(self, success: bool) -> bool:
        with self._lock:
            self._order_attempts += 1
            if not success:
                self._order_rejections += 1
            
            if self._order_attempts == 0:
                return True
            
            rejection_rate = self._order_rejections / self._order_attempts
            
            return self._breakers["order_rejection"].check(
                value=rejection_rate,
                threshold=self._config.max_order_rejection_rate,
                context={"rejection_rate": rejection_rate, "attempts": self._order_attempts, "rejections": self._order_rejections},
            )
    
    def record_latency(self, latency_ms: float) -> bool:
        with self._lock:
            self._latency_samples.append(latency_ms)
            
            if len(self._latency_samples) < 10:
                return True
            
            avg_latency = sum(self._latency_samples) / len(self._latency_samples)
            
            return self._breakers["latency"].check(
                value=avg_latency,
                threshold=self._config.max_latency_ms,
                context={"avg_latency_ms": avg_latency, "samples": len(self._latency_samples)},
            )
    
    def is_trading_allowed(self) -> bool:
        with self._lock:
            if self._emergency_shutdown:
                return False
            
            for name, breaker in self._breakers.items():
                if breaker.get_state() == CircuitBreakerState.OPEN:
                    return False
            
            return True
    
    def get_global_safety_level(self) -> SafetyLevel:
        with self._lock:
            return self._global_safety_level
    
    def reset_breaker(self, name: str) -> None:
        with self._lock:
            if name in self._breakers:
                self._breakers[name].reset()
                if self._emergency_shutdown:
                    self._emergency_shutdown = False
                    self._global_safety_level = SafetyLevel.NORMAL
    
    def reset_all_breakers(self) -> None:
        with self._lock:
            for breaker in self._breakers.values():
                breaker.reset()
            self._emergency_shutdown = False
            self._global_safety_level = SafetyLevel.NORMAL
            logger.info("All circuit breakers reset")
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "global_safety_level": self._global_safety_level.value,
                "emergency_shutdown": self._emergency_shutdown,
                "trading_allowed": self.is_trading_allowed(),
                "initial_equity": self._initial_equity,
                "current_equity": self._current_equity,
                "order_attempts": self._order_attempts,
                "order_rejections": self._order_rejections,
                "breakers": {name: breaker.get_stats() for name, breaker in self._breakers.items()},
            }
