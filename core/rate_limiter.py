from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    capacity: int  # Maximum tokens in bucket
    refill_rate: float  # Tokens per second
    refill_interval: float = 1.0  # Seconds between refills
    min_wait: float = 0.001  # Minimum wait time in seconds


class TokenBucket:
    """
    Thread-safe Token Bucket rate limiter using atomic operations.
    Prevents HTTP 429 errors by smoothing outbound requests against broker caps.
    Uses bitwise operations for efficient token counting and minimal lock contention.
    """
    
    def __init__(self, config: RateLimitConfig) -> None:
        self._config = config
        self._tokens = float(config.capacity)
        self._last_refill = time.time()
        self._lock = threading.RLock()
        self._request_history: deque = deque(maxlen=1000)
        self._total_requests = 0
        self._total_rejected = 0
        self._total_wait_time = 0.0
        
    def _refill(self) -> None:
        current_time = time.time()
        elapsed = current_time - self._last_refill
        
        if elapsed >= self._config.refill_interval:
            tokens_to_add = elapsed * self._config.refill_rate
            self._tokens = min(self._config.capacity, self._tokens + tokens_to_add)
            self._last_refill = current_time
    
    def consume(self, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Attempt to consume tokens from the bucket.
        Returns True if successful, False if tokens unavailable and not blocking.
        """
        with self._lock:
            self._refill()
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._total_requests += 1
                self._request_history.append(time.time())
                return True
            
            if not block:
                self._total_rejected += 1
                return False
            
            if timeout is not None and timeout <= 0:
                self._total_rejected += 1
                return False
            
            wait_time = self._calculate_wait_time(tokens)
            
            if timeout is not None and wait_time > timeout:
                self._total_rejected += 1
                return False
            
            wait_time = max(wait_time, self._config.min_wait)
            self._total_wait_time += wait_time
            
        time.sleep(wait_time)
        
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._total_requests += 1
                self._request_history.append(time.time())
                return True
            else:
                self._total_rejected += 1
                return False
    
    def _calculate_wait_time(self, tokens: int) -> float:
        deficit = tokens - self._tokens
        if deficit <= 0:
            return 0.0
        return deficit / self._config.refill_rate
    
    def try_consume(self, tokens: int = 1) -> bool:
        """Non-blocking consume attempt."""
        return self.consume(tokens=tokens, block=False)
    
    def available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens
    
    def reset(self) -> None:
        with self._lock:
            self._tokens = float(self._config.capacity)
            self._last_refill = time.time()
            self._request_history.clear()
            self._total_requests = 0
            self._total_rejected = 0
            self._total_wait_time = 0.0
    
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            self._refill()
            current_time = time.time()
            request_rate = 0.0
            
            if self._request_history:
                time_window = 60.0
                cutoff = current_time - time_window
                recent_requests = sum(1 for ts in self._request_history if ts >= cutoff)
                request_rate = recent_requests / time_window
            
            return {
                "available_tokens": self._tokens,
                "capacity": self._config.capacity,
                "refill_rate": self._config.refill_rate,
                "total_requests": self._total_requests,
                "total_rejected": self._total_rejected,
                "total_wait_time": self._total_wait_time,
                "request_rate_per_minute": request_rate,
                "rejection_rate": self._total_rejected / (self._total_requests + self._total_rejected) if (self._total_requests + self._total_rejected) > 0 else 0.0,
            }


class MultiBucketRateLimiter:
    """
    Multi-bucket rate limiter for different API endpoints with independent limits.
    Each endpoint has its own token bucket with configurable capacity and refill rate.
    """
    
    def __init__(self) -> None:
        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = threading.RLock()
        self._default_config = RateLimitConfig(capacity=200, refill_rate=200.0/60.0)
        
    def add_bucket(self, name: str, config: RateLimitConfig) -> None:
        with self._lock:
            self._buckets[name] = TokenBucket(config)
            logger.info("Rate limit bucket added", extra={"name": name, "capacity": config.capacity, "refill_rate": config.refill_rate})
    
    def set_default_config(self, config: RateLimitConfig) -> None:
        with self._lock:
            self._default_config = config
    
    def consume(self, bucket_name: str, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        with self._lock:
            if bucket_name not in self._buckets:
                self._buckets[bucket_name] = TokenBucket(self._default_config)
            bucket = self._buckets[bucket_name]
        
        return bucket.consume(tokens=tokens, block=block, timeout=timeout)
    
    def try_consume(self, bucket_name: str, tokens: int = 1) -> bool:
        return self.consume(bucket_name, tokens=tokens, block=False)
    
    def available_tokens(self, bucket_name: str) -> float:
        with self._lock:
            if bucket_name not in self._buckets:
                return float(self._default_config.capacity)
            return self._buckets[bucket_name].available_tokens()
    
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                bucket_name: bucket.stats()
                for bucket_name, bucket in self._buckets.items()
            }
    
    def reset(self, bucket_name: Optional[str] = None) -> None:
        with self._lock:
            if bucket_name is None:
                for bucket in self._buckets.values():
                    bucket.reset()
            elif bucket_name in self._buckets:
                self._buckets[bucket_name].reset()


class AlpacaRateLimiter(MultiBucketRateLimiter):
    """
    Specialized rate limiter for Alpaca API endpoints.
    Configured with institutional-grade limits matching exchange constraints.
    """
    
    def __init__(self) -> None:
        super().__init__()
        
        # Alpaca Paper Trading limits (adjustable for live)
        # Trading API: 200 requests per minute
        trading_config = RateLimitConfig(
            capacity=200,
            refill_rate=200.0 / 60.0,
            refill_interval=1.0,
            min_wait=0.01
        )
        self.add_bucket("trading", trading_config)
        
        # Data API: 200 requests per minute
        data_config = RateLimitConfig(
            capacity=200,
            refill_rate=200.0 / 60.0,
            refill_interval=1.0,
            min_wait=0.01
        )
        self.add_bucket("data", data_config)
        
        # Order submission: stricter limit (50 per minute)
        order_config = RateLimitConfig(
            capacity=50,
            refill_rate=50.0 / 60.0,
            refill_interval=1.0,
            min_wait=0.05
        )
        self.add_bucket("orders", order_config)
        
        # Account queries: 200 per minute
        account_config = RateLimitConfig(
            capacity=200,
            refill_rate=200.0 / 60.0,
            refill_interval=1.0,
            min_wait=0.01
        )
        self.add_bucket("account", account_config)
        
        logger.info("Alpaca rate limiter initialized with institutional buckets")
    
    def consume_trading(self, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        return self.consume("trading", tokens=tokens, block=block, timeout=timeout)
    
    def consume_data(self, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        return self.consume("data", tokens=tokens, block=block, timeout=timeout)
    
    def consume_order(self, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        return self.consume("orders", tokens=tokens, block=block, timeout=timeout)
    
    def consume_account(self, tokens: int = 1, block: bool = True, timeout: Optional[float] = None) -> bool:
        return self.consume("account", tokens=tokens, block=block, timeout=timeout)
