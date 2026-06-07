from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests

from config.settings import Settings
from .backoff import retry

logger = logging.getLogger(__name__)


class BrokerClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._session = requests.Session()
        # Default headers for Alpaca
        self._session.headers.update(
            {
                "APCA-API-KEY-ID": settings.alpaca_api_key or "",
                "APCA-API-SECRET-KEY": settings.alpaca_api_secret or "",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _url(self, base: str, path: str) -> str:
        path = path if path.startswith("/") else f"/{path}"
        return f"{base}{path}"

    @retry(exceptions=(requests.RequestException,), max_attempts=5)
    def get_account(self) -> Optional[Dict[str, Any]]:
        url = self._url(self.settings.alpaca_trading_base_url, "/v2/account")
        try:
            r = self._session.get(url, timeout=10)
            if r.status_code == 429:
                logger.warning("Rate limited by broker (429)")
                r.raise_for_status()
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error("get_account failed: %s", e, exc_info=True)
            raise

    @retry(exceptions=(requests.RequestException,), max_attempts=5)
    def get_bars(self, symbol: str, timeframe: str = "1Day", limit: int = 50) -> Optional[Dict[str, Any]]:
        # Using Alpaca Data API v2
        path = f"/v2/stocks/{symbol}/bars?timeframe={timeframe}&limit={limit}"
        url = self._url(self.settings.alpaca_data_base_url, path)
        try:
            r = self._session.get(url, timeout=10)
            if r.status_code == 429:
                logger.warning("Rate limited by data API (429)")
                r.raise_for_status()
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error("get_bars failed: %s", e, exc_info=True)
            raise

    @retry(exceptions=(requests.RequestException,), max_attempts=5)
    def get_latest_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        path = f"/v2/stocks/{symbol}/quotes/latest"
        url = self._url(self.settings.alpaca_data_base_url, path)
        try:
            r = self._session.get(url, timeout=10)
            if r.status_code == 429:
                logger.warning("Rate limited by data API (429)")
                r.raise_for_status()
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error("get_latest_quote failed: %s", e, exc_info=True)
            raise

    @retry(exceptions=(requests.RequestException,), max_attempts=5)
    def submit_bracket_order(
        self,
        *,
        symbol: str,
        qty: float,
        side: str,
        limit_price: float,
        take_profit_price: float,
        stop_loss_price: float,
        time_in_force: str = "gtc",
    ) -> Optional[Dict[str, Any]]:
        if self.settings.environment_mode == "SHADOW":
            # Simulate immediate fill for shadow mode
            mock = {
                "id": f"shadow-{symbol}-{int(limit_price*100)}",
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": "limit",
                "order_class": "bracket",
                "limit_price": limit_price,
                "take_profit": {"limit_price": take_profit_price},
                "stop_loss": {"stop_price": stop_loss_price},
                "status": "filled",
            }
            logger.info("SHADOW bracket order simulated", extra=mock)
            return mock
        url = self._url(self.settings.alpaca_trading_base_url, "/v2/orders")
        body = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": "limit",
            "time_in_force": time_in_force,
            "limit_price": round(float(limit_price), 4),
            "order_class": "bracket",
            "take_profit": {"limit_price": round(float(take_profit_price), 4)},
            "stop_loss": {"stop_price": round(float(stop_loss_price), 4)},
        }
        try:
            r = self._session.post(url, json=body, timeout=10)
            if r.status_code == 429:
                logger.warning("Rate limited by broker on order submit (429)")
                r.raise_for_status()
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error("submit_bracket_order failed: %s", e, exc_info=True)
            raise

    @retry(exceptions=(requests.RequestException,), max_attempts=5)
    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        if self.settings.environment_mode == "SHADOW":
            # In SHADOW, treat orders as already filled
            return {"id": order_id, "status": "filled"}
        url = self._url(self.settings.alpaca_trading_base_url, f"/v2/orders/{order_id}")
        try:
            r = self._session.get(url, timeout=10)
            if r.status_code == 429:
                logger.warning("Rate limited on get_order (429)")
                r.raise_for_status()
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error("get_order failed: %s", e, exc_info=True)
            raise

    @retry(exceptions=(requests.RequestException,), max_attempts=5)
    def cancel_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        if self.settings.environment_mode == "SHADOW":
            return {"id": order_id, "status": "canceled"}
        url = self._url(self.settings.alpaca_trading_base_url, f"/v2/orders/{order_id}")
        try:
            r = self._session.delete(url, timeout=10)
            if r.status_code == 429:
                logger.warning("Rate limited on cancel_order (429)")
                r.raise_for_status()
            r.raise_for_status()
            return r.json() if r.text else {"id": order_id, "status": "canceled"}
        except requests.RequestException as e:
            logger.error("cancel_order failed: %s", e, exc_info=True)
            raise
