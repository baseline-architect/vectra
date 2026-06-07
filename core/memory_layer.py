from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable
from queue import Empty, Queue
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from sqlalchemy.orm import Session

from models.database import session_scope

logger = logging.getLogger(__name__)


@runtime_checkable
@dataclass
class CacheEntry:
    key: str
    value: Any
    timestamp: float
    ttl_seconds: Optional[float] = None

    def is_expired(self) -> bool:
        if self.ttl_seconds is None:
            return False
        return (time.time() - self.timestamp) > self.ttl_seconds


@runtime_checkable
@dataclass
class WriteOperation:
    operation: str  # "upsert", "delete"
    table: str
    key: str
    data: Optional[Dict[str, Any]] = None
    callback: Optional[Callable[[bool], None]] = None


class AsyncMemoryCache:
    """
    Zero-latency in-memory cache with concurrent deque-based ring buffers.
    Provides atomic read/write operations without blocking the main execution loop.
    """
    
    def __init__(self, max_size: int = 10000, default_ttl: float = 300.0) -> None:
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._access_queue: deque = deque(maxlen=1000)
        self._hit_count = 0
        self._miss_count = 0
        
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._miss_count += 1
                return None
            if entry.is_expired():
                del self._cache[key]
                self._miss_count += 1
                return None
            self._hit_count += 1
            self._access_queue.append(key)
            return entry.value
    
    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        with self._lock:
            if len(self._cache) >= self._max_size and key not in self._cache:
                self._evict_lru()
            entry = CacheEntry(
                key=key,
                value=value,
                timestamp=time.time(),
                ttl_seconds=ttl if ttl is not None else self._default_ttl
            )
            self._cache[key] = entry
            self._access_queue.append(key)
    
    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def _evict_lru(self) -> None:
        if not self._access_queue:
            return
        while self._access_queue:
            key = self._access_queue.popleft()
            if key in self._cache:
                del self._cache[key]
                break
    
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._access_queue.clear()
            self._hit_count = 0
            self._miss_count = 0
    
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hit_count + self._miss_count
            hit_rate = self._hit_count / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self._max_size,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_rate": hit_rate,
            }


class BackgroundDBWorker:
    """
    Isolated background worker thread for lazy, batch-written ACID commits.
    Decouples database I/O from the main execution loop for zero-latency operations.
    """
    
    def __init__(self, batch_size: int = 50, flush_interval: float = 1.0) -> None:
        self._queue: Queue[WriteOperation] = Queue(maxsize=10000)
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db_worker")
        self._pending_operations: list[WriteOperation] = []
        self._last_flush = time.time()
        self._total_processed = 0
        self._total_failed = 0
        
    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker_loop, daemon=True, name="BackgroundDBWorker")
        self._thread.start()
        logger.info("Background DB worker started", extra={"batch_size": self._batch_size, "flush_interval": self._flush_interval})
    
    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        self._executor.shutdown(wait=True)
        logger.info("Background DB worker stopped")
    
    def enqueue(self, operation: WriteOperation) -> bool:
        try:
            self._queue.put(operation, block=False)
            return True
        except Exception:
            logger.warning("DB worker queue full, dropping operation", extra={"operation": operation.operation})
            return False
    
    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                op = self._queue.get(timeout=0.1)
                self._pending_operations.append(op)
            except Empty:
                pass
            
            current_time = time.time()
            should_flush = (
                len(self._pending_operations) >= self._batch_size or
                (current_time - self._last_flush) >= self._flush_interval
            )
            
            if should_flush and self._pending_operations:
                self._flush_batch()
                self._last_flush = current_time
        
        if self._pending_operations:
            self._flush_batch()
    
    def _flush_batch(self) -> None:
        if not self._pending_operations:
            return
        
        batch = self._pending_operations[:]
        self._pending_operations.clear()
        
        try:
            with session_scope() as session:
                for op in batch:
                    try:
                        self._execute_operation(session, op)
                        self._total_processed += 1
                        if op.callback:
                            op.callback(True)
                    except Exception as e:
                        logger.error("DB operation failed", extra={"operation": op.operation, "key": op.key}, exc_info=True)
                        self._total_failed += 1
                        if op.callback:
                            op.callback(False)
        except Exception as e:
            logger.error("DB batch commit failed", extra={"batch_size": len(batch)}, exc_info=True)
            self._total_failed += len(batch)
            for op in batch:
                if op.callback:
                    op.callback(False)
    
    def _execute_operation(self, session: Session, op: WriteOperation) -> None:
        if op.operation == "upsert" and op.data:
            from models.position import Position
            from models.metrics import StrategyMetric
            
            if op.table == "positions":
                pos = session.query(Position).filter_by(symbol=op.key).first()
                if pos:
                    pos.qty = op.data.get("qty", pos.qty)
                    pos.entry_price = op.data.get("entry_price", pos.entry_price)
                    pos.stop_loss = op.data.get("stop_loss", pos.stop_loss)
                    pos.take_profit = op.data.get("take_profit", pos.take_profit)
                else:
                    pos = Position(
                        symbol=op.key,
                        qty=op.data.get("qty", 0),
                        entry_price=op.data.get("entry_price", 0.0),
                        stop_loss=op.data.get("stop_loss"),
                        take_profit=op.data.get("take_profit"),
                    )
                    session.add(pos)
            
            elif op.table == "metrics":
                metric = session.query(StrategyMetric).filter_by(symbol=op.key).first()
                if metric:
                    metric.win_rate = op.data.get("win_rate", metric.win_rate)
                    metric.win_loss_ratio = op.data.get("win_loss_ratio", metric.win_loss_ratio)
                else:
                    metric = StrategyMetric(
                        symbol=op.key,
                        win_rate=op.data.get("win_rate", 0.5),
                        win_loss_ratio=op.data.get("win_loss_ratio", 1.0),
                    )
                    session.add(metric)
        
        elif op.operation == "delete":
            if op.table == "positions":
                from models.position import Position
                pos = session.query(Position).filter_by(symbol=op.key).first()
                if pos:
                    session.delete(pos)
            elif op.table == "metrics":
                from models.metrics import StrategyMetric
                metric = session.query(StrategyMetric).filter_by(symbol=op.key).first()
                if metric:
                    session.delete(metric)
    
    def stats(self) -> Dict[str, Any]:
        return {
            "queue_size": self._queue.qsize(),
            "pending_size": len(self._pending_operations),
            "total_processed": self._total_processed,
            "total_failed": self._total_failed,
        }


class HybridMemoryLayer:
    """
    Unified memory layer combining zero-latency cache with background DB persistence.
    Main execution loop reads/writes to cache only; DB worker handles persistence asynchronously.
    """
    
    def __init__(self, cache_max_size: int = 10000, db_batch_size: int = 50, db_flush_interval: float = 1.0) -> None:
        self.cache = AsyncMemoryCache(max_size=cache_max_size)
        self.db_worker = BackgroundDBWorker(batch_size=db_batch_size, flush_interval=db_flush_interval)
        self._initialized = False
    
    def initialize(self) -> None:
        if self._initialized:
            return
        self.db_worker.start()
        self._initialized = True
        logger.info("Hybrid memory layer initialized")
    
    def shutdown(self) -> None:
        if not self._initialized:
            return
        self.db_worker.stop()
        self.cache.clear()
        self._initialized = False
    
    def get(self, key: str) -> Optional[Any]:
        return self.cache.get(key)
    
    def set(self, key: str, value: Any, persist: bool = True, ttl: Optional[float] = None) -> None:
        self.cache.set(key, value, ttl=ttl)
        if persist:
            op = WriteOperation(operation="upsert", table="cache", key=key, data={"value": value})
            self.db_worker.enqueue(op)
    
    def delete(self, key: str, persist: bool = True) -> bool:
        deleted = self.cache.delete(key)
        if persist and deleted:
            op = WriteOperation(operation="delete", table="cache", key=key)
            self.db_worker.enqueue(op)
        return deleted
    
    def upsert_position(self, symbol: str, qty: float, entry_price: float, stop_loss: Optional[float], take_profit: Optional[float]) -> None:
        cache_key = f"position:{symbol}"
        data = {"symbol": symbol, "qty": qty, "entry_price": entry_price, "stop_loss": stop_loss, "take_profit": take_profit}
        self.cache.set(cache_key, data)
        op = WriteOperation(operation="upsert", table="positions", key=symbol, data=data)
        self.db_worker.enqueue(op)
    
    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        cache_key = f"position:{symbol}"
        return self.cache.get(cache_key)
    
    def delete_position(self, symbol: str) -> None:
        cache_key = f"position:{symbol}"
        self.cache.delete(cache_key)
        op = WriteOperation(operation="delete", table="positions", key=symbol)
        self.db_worker.enqueue(op)
    
    def upsert_metric(self, symbol: str, win_rate: float, win_loss_ratio: float) -> None:
        cache_key = f"metric:{symbol}"
        data = {"symbol": symbol, "win_rate": win_rate, "win_loss_ratio": win_loss_ratio}
        self.cache.set(cache_key, data)
        op = WriteOperation(operation="upsert", table="metrics", key=symbol, data=data)
        self.db_worker.enqueue(op)
    
    def get_metric(self, symbol: str) -> Optional[Dict[str, Any]]:
        cache_key = f"metric:{symbol}"
        return self.cache.get(cache_key)
    
    def stats(self) -> Dict[str, Any]:
        return {
            "cache": self.cache.stats(),
            "db_worker": self.db_worker.stats(),
        }
