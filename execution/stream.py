from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Iterable, List

import websocket  # websocket-client

from config.settings import Settings

logger = logging.getLogger(__name__)


class AlpacaStream:
    def __init__(self, settings: Settings, symbols: Iterable[str], on_bar: Callable[[str, dict], None]) -> None:
        self.settings = settings
        self.symbols: List[str] = [s.upper() for s in symbols]
        self.on_bar = on_bar
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._stop = False

    def _on_open(self, ws: websocket.WebSocketApp) -> None:  # type: ignore[name-defined]
        logger.info("WebSocket opened; sending auth and subscribe")
        auth = {"action": "auth", "key": self.settings.alpaca_api_key or "", "secret": self.settings.alpaca_api_secret or ""}
        ws.send(json.dumps(auth))
        sub = {"action": "subscribe", "bars": self.symbols}
        ws.send(json.dumps(sub))

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:  # type: ignore[name-defined]
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        # Messages may be a list of events
        events = data if isinstance(data, list) else [data]
        for ev in events:
            if isinstance(ev, dict) and ev.get("T") == "b":  # bar event
                symbol = ev.get("S")
                if symbol in self.symbols:
                    self.on_bar(symbol, ev)

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:  # type: ignore[name-defined]
        logger.error("WebSocket error: %s", error, exc_info=True)

    def _on_close(self, ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:  # type: ignore[name-defined]
        logger.warning("WebSocket closed", extra={"code": close_status_code, "msg": close_msg})

    def start(self) -> None:
        self._stop = False
        def run():
            while not self._stop:
                try:
                    self._ws = websocket.WebSocketApp(
                        self.settings.alpaca_stream_url,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                    )
                    self._ws.run_forever(ping_interval=20, ping_timeout=10)
                except Exception as e:
                    logger.error("WebSocket run_forever error: %s", e, exc_info=True)
                # Backoff before reconnect
                time.sleep(3)
        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
