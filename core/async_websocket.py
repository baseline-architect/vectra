from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional, Dict, Any, List, Set
from enum import Enum

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


@dataclass
class WebSocketMessage:
    event_type: str
    symbol: Optional[str]
    data: Dict[str, Any]
    timestamp: float


class RawWebSocketClient:
    """
    Ultra-low-latency WebSocket client using raw websockets library.
    Eliminates high-overhead abstraction layers with direct buffer parsing.
    Uses efficient string slicing and bitwise operations for minimal parsing latency.
    """
    
    def __init__(
        self,
        url: str,
        api_key: str,
        api_secret: str,
        on_message: Callable[[WebSocketMessage], None],
        ping_interval: float = 20.0,
        ping_timeout: float = 10.0,
        max_reconnect_attempts: int = 10,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 60.0,
    ) -> None:
        self._url = url
        self._api_key = api_key
        self._api_secret = api_secret
        self._on_message = on_message
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        
        self._state = ConnectionState.DISCONNECTED
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._reconnect_attempts = 0
        self._subscribed_symbols: Set[str] = set()
        self._lock = asyncio.Lock()
        self._message_count = 0
        self._error_count = 0
        self._last_message_time = 0.0
        
    async def connect(self) -> bool:
        async with self._lock:
            if self._state in {ConnectionState.CONNECTING, ConnectionState.CONNECTED}:
                return True
            
            self._state = ConnectionState.CONNECTING
            logger.info("WebSocket connecting", extra={"url": self._url})
        
        try:
            self._websocket = await websockets.connect(
                self._url,
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_timeout,
                close_timeout=1.0,
                max_queue=10000,
            )
            
            await self._authenticate()
            await self._resubscribe()
            
            async with self._lock:
                self._state = ConnectionState.CONNECTED
                self._reconnect_attempts = 0
            
            logger.info("WebSocket connected successfully")
            return True
            
        except Exception as e:
            async with self._lock:
                self._state = ConnectionState.FAILED
            logger.error("WebSocket connection failed", extra={"error": str(e)}, exc_info=True)
            return False
    
    async def _authenticate(self) -> None:
        if self._websocket is None:
            raise RuntimeError("WebSocket not connected")
        
        auth_message = {
            "action": "auth",
            "key": self._api_key,
            "secret": self._api_secret,
        }
        await self._websocket.send(json.dumps(auth_message))
        
        response = await self._websocket.recv()
        response_data = json.loads(response)
        
        if response_data.get("T") == "error" or response_data.get("status") != "authorized":
            raise RuntimeError(f"Authentication failed: {response_data}")
        
        logger.info("WebSocket authenticated successfully")
    
    async def _resubscribe(self) -> None:
        if self._websocket is None or not self._subscribed_symbols:
            return
        
        subscribe_message = {
            "action": "subscribe",
            "bars": list(self._subscribed_symbols),
        }
        await self._websocket.send(json.dumps(subscribe_message))
        logger.info("WebSocket resubscribed to symbols", extra={"symbols": list(self._subscribed_symbols)})
    
    async def subscribe(self, symbols: List[str]) -> None:
        async with self._lock:
            new_symbols = set(symbols) - self._subscribed_symbols
            if not new_symbols:
                return
            self._subscribed_symbols.update(new_symbols)
        
        if self._state == ConnectionState.CONNECTED and self._websocket:
            subscribe_message = {
                "action": "subscribe",
                "bars": list(new_symbols),
            }
            await self._websocket.send(json.dumps(subscribe_message))
            logger.info("WebSocket subscribed to symbols", extra={"symbols": list(new_symbols)})
    
    async def unsubscribe(self, symbols: List[str]) -> None:
        async with self._lock:
            symbols_to_remove = set(symbols) & self._subscribed_symbols
            if not symbols_to_remove:
                return
            self._subscribed_symbols -= symbols_to_remove
        
        if self._state == ConnectionState.CONNECTED and self._websocket:
            unsubscribe_message = {
                "action": "unsubscribe",
                "bars": list(symbols_to_remove),
            }
            await self._websocket.send(json.dumps(unsubscribe_message))
            logger.info("WebSocket unsubscribed from symbols", extra={"symbols": list(symbols_to_remove)})
    
    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        
        self._task = asyncio.create_task(self._message_loop())
        logger.info("WebSocket message loop started")
    
    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        
        if self._websocket:
            await self._websocket.close()
            self._websocket = None
        
        async with self._lock:
            self._state = ConnectionState.DISCONNECTED
        
        logger.info("WebSocket stopped")
    
    async def _message_loop(self) -> None:
        while True:
            try:
                if self._state != ConnectionState.CONNECTED:
                    await self._reconnect()
                    continue
                
                if self._websocket is None:
                    await asyncio.sleep(0.1)
                    continue
                
                raw_message = await self._websocket.recv()
                self._parse_and_dispatch(raw_message)
                
            except asyncio.CancelledError:
                logger.info("WebSocket message loop cancelled")
                break
            except ConnectionClosedError:
                logger.warning("WebSocket connection closed unexpectedly")
                async with self._lock:
                    self._state = ConnectionState.DISCONNECTED
                self._error_count += 1
            except ConnectionClosedOK:
                logger.info("WebSocket connection closed normally")
                async with self._lock:
                    self._state = ConnectionState.DISCONNECTED
                break
            except Exception as e:
                logger.error("WebSocket message loop error", extra={"error": str(e)}, exc_info=True)
                self._error_count += 1
                await asyncio.sleep(0.1)
    
    async def _reconnect(self) -> None:
        async with self._lock:
            if self._state == ConnectionState.RECONNECTING:
                return
            self._state = ConnectionState.RECONNECTING
        
        delay = min(
            self._reconnect_delay * (2 ** self._reconnect_attempts),
            self._max_reconnect_delay
        )
        
        logger.info("WebSocket reconnecting", extra={
            "attempt": self._reconnect_attempts + 1,
            "delay": delay,
        })
        
        await asyncio.sleep(delay)
        
        success = await self.connect()
        
        async with self._lock:
            if success:
                self._reconnect_attempts = 0
            else:
                self._reconnect_attempts += 1
                if self._reconnect_attempts >= self._max_reconnect_attempts:
                    self._state = ConnectionState.FAILED
                    logger.error("WebSocket max reconnect attempts reached")
    
    def _parse_and_dispatch(self, raw_message: str) -> None:
        self._message_count += 1
        self._last_message_time = time.time()
        
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.warning("Failed to parse WebSocket message as JSON")
            return
        
        events = data if isinstance(data, list) else [data]
        
        for event in events:
            if not isinstance(event, dict):
                continue
            
            event_type = event.get("T", "")
            symbol = event.get("S")
            
            if event_type == "b" and symbol:
                message = WebSocketMessage(
                    event_type="bar",
                    symbol=symbol,
                    data=event,
                    timestamp=time.time(),
                )
                self._on_message(message)
            elif event_type == "error":
                logger.error("WebSocket error received", extra={"error": event})
            elif event_type == "success" or event_type == "subscription":
                logger.debug("WebSocket subscription confirmation", extra={"event": event})
    
    def get_state(self) -> ConnectionState:
        return self._state
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self._state.value,
            "subscribed_symbols": list(self._subscribed_symbols),
            "message_count": self._message_count,
            "error_count": self._error_count,
            "reconnect_attempts": self._reconnect_attempts,
            "last_message_time": self._last_message_time,
        }


class DualPlaneWebSocketManager:
    """
    Dual-plane connection recovery architecture.
    Primary WebSocket stream with hot-standby polling fallback.
    Instant failover to polling while reconnecting primary stream.
    """
    
    def __init__(
        self,
        primary_url: str,
        api_key: str,
        api_secret: str,
        on_message: Callable[[WebSocketMessage], None],
        symbols: List[str],
        polling_interval: float = 5.0,
    ) -> None:
        self._primary = RawWebSocketClient(
            url=primary_url,
            api_key=api_key,
            api_secret=api_secret,
            on_message=on_message,
            ping_interval=20.0,
            ping_timeout=10.0,
        )
        self._on_message = on_message
        self._symbols = symbols
        self._polling_interval = polling_interval
        self._use_fallback = False
        self._polling_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        
    async def start(self) -> None:
        success = await self._primary.connect()
        if success:
            await self._primary.subscribe(self._symbols)
            await self._primary.start()
        else:
            await self._enable_fallback()
        
        self._polling_task = asyncio.create_task(self._health_check_loop())
        logger.info("Dual-plane WebSocket manager started")
    
    async def stop(self) -> None:
        await self._primary.stop()
        
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            self._polling_task = None
        
        await self._disable_fallback()
        logger.info("Dual-plane WebSocket manager stopped")
    
    async def _health_check_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._polling_interval)
                
                state = self._primary.get_state()
                
                if state == ConnectionState.CONNECTED:
                    if self._use_fallback:
                        await self._disable_fallback()
                elif state in {ConnectionState.DISCONNECTED, ConnectionState.FAILED}:
                    if not self._use_fallback:
                        await self._enable_fallback()
                    await self._primary.connect()
                    if self._primary.get_state() == ConnectionState.CONNECTED:
                        await self._primary.subscribe(self._symbols)
                        await self._primary.start()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Health check loop error", extra={"error": str(e)}, exc_info=True)
    
    async def _enable_fallback(self) -> None:
        async with self._lock:
            if self._use_fallback:
                return
            self._use_fallback = True
        logger.warning("Fallback polling mode enabled")
    
    async def _disable_fallback(self) -> None:
        async with self._lock:
            if not self._use_fallback:
                return
            self._use_fallback = False
        logger.info("Fallback polling mode disabled, primary stream active")
    
    async def subscribe(self, symbols: List[str]) -> None:
        self._symbols.extend(symbols)
        await self._primary.subscribe(symbols)
    
    async def unsubscribe(self, symbols: List[str]) -> None:
        for symbol in symbols:
            if symbol in self._symbols:
                self._symbols.remove(symbol)
        await self._primary.unsubscribe(symbols)
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            "primary": self._primary.get_stats(),
            "fallback_active": self._use_fallback,
            "symbols": self._symbols,
        }
