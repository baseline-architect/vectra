from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from config.settings import Settings

logger = logging.getLogger(__name__)


@dataclass
class TelemetryMessage:
    title: str
    description: str
    fields: Dict[str, object]


class TelemetryService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._session = requests.Session()
        self._last_heartbeat = time.time()
        self._stop = False

    def send(self, message: TelemetryMessage) -> None:
        payload = {
            "title": message.title,
            "description": message.description,
            "fields": message.fields,
        }
        for url in self.settings.telemetry_webhook_urls:
            try:
                r = self._session.post(url, json=payload, timeout=10)
                r.raise_for_status()
            except Exception as e:
                logger.error("Telemetry send failed: %s", e, exc_info=True)

    def heartbeat(self) -> None:
        self._last_heartbeat = time.time()

    def start_deadman(self) -> None:
        if not self.settings.healthcheck_url:
            return
        t = threading.Thread(target=self._deadman_loop, daemon=True)
        t.start()

    def _deadman_loop(self) -> None:
        while not self._stop:
            now = time.time()
            if now - self._last_heartbeat > self.settings.deadman_timeout_seconds:
                try:
                    self._session.get(self.settings.healthcheck_url, timeout=10)
                    logger.error("DEADMAN triggered: heartbeat overdue", extra={"overdue_seconds": now - self._last_heartbeat})
                except Exception as e:
                    logger.error("Deadman ping failed: %s", e, exc_info=True)
                # After triggering, reset timer to avoid spamming
                self._last_heartbeat = now
            time.sleep(max(5, min(self.settings.heartbeat_interval_seconds, 60)))

    def stop(self) -> None:
        self._stop = True
