from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

from execution.broker import BrokerClient

logger = logging.getLogger(__name__)


@dataclass
class ChaseResult:
    order_id: Optional[str]
    filled: bool


class PassiveLimitChaser:
    def __init__(self, broker: BrokerClient, poll_seconds: int = 30, on_filled: Optional[Callable[[str, str, float, float, float, float], None]] = None) -> None:
        self.broker = broker
        self.poll_seconds = poll_seconds
        self._threads: dict[str, threading.Thread] = {}
        self._on_filled = on_filled

    def start_chase(self, symbol: str, qty: float, side: str, take_profit_pct: float, stop_loss_pct: float) -> None:
        if symbol in self._threads and self._threads[symbol].is_alive():
            logger.info("Chase already active", extra={"symbol": symbol})
            return
        t = threading.Thread(target=self._chase_loop, args=(symbol, qty, side, take_profit_pct, stop_loss_pct), daemon=True)
        self._threads[symbol] = t
        t.start()

    def _best_bid(self, symbol: str) -> Optional[float]:
        q = self.broker.get_latest_quote(symbol)
        # Alpaca latest quotes use structure: {"symbol":"AAPL","quote":{"bp": bid_price, "ap": ask_price, ...}}
        try:
            quote = (q or {}).get("quote", {})
            bp = float(quote.get("bp"))
            return bp if bp > 0 else None
        except Exception:
            return None

    def _chase_loop(self, symbol: str, qty: float, side: str, take_profit_pct: float, stop_loss_pct: float) -> None:
        order_id: Optional[str] = None
        while True:
            price = self._best_bid(symbol)
            if price is None:
                logger.warning("No bid available for chaser", extra={"symbol": symbol})
                time.sleep(self.poll_seconds)
                continue
            # compute TP/SL from entry
            tp = round(price * (1 + take_profit_pct), 4)
            sl = round(price * (1 - stop_loss_pct), 4)
            # Place new bracket limit order
            res = self.broker.submit_bracket_order(
                symbol=symbol,
                qty=qty,
                side=side,
                limit_price=price,
                take_profit_price=tp,
                stop_loss_price=sl,
            )
            order_id = (res or {}).get("id")
            logger.info("Bracket limit placed", extra={"symbol": symbol, "order_id": order_id, "limit": price, "tp": tp, "sl": sl})

            # Poll until next cycle
            time.sleep(self.poll_seconds)
            if order_id:
                status = (self.broker.get_order(order_id) or {}).get("status")
                if status in {"filled", "partially_filled"}:
                    logger.info("Order filled; stopping chase", extra={"symbol": symbol, "order_id": order_id, "status": status})
                    if self._on_filled is not None:
                        try:
                            self._on_filled(symbol, str(order_id), float(price), float(tp), float(sl), float(qty))
                        except Exception:
                            logger.error("on_filled callback error", exc_info=True)
                    return
                # Cancel and chase at new bid
                self.broker.cancel_order(order_id)
                logger.info("Order canceled for chase", extra={"symbol": symbol, "order_id": order_id})
