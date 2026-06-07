from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional

import pandas as pd

from config.logging_config import configure_logging
from config.settings import load_settings, Settings
from models.database import init_engine, create_all, session_scope
from models.repository import (
    get_all_positions,
    get_position,
    upsert_position,
    delete_position,
)
from execution.broker import BrokerClient
from strategy.data import bars_json_to_df, resample_weekly
from strategy.volatility import VolatilityRegimeFilter
from strategy.multi_timeframe import validate_golden_cross
from strategy.correlation import correlation_protector
from strategy.macro_gatekeeper import parse_events, should_block
from strategy.kelly import compute_kelly_for_symbol
from execution.chaser import PassiveLimitChaser
from execution.telemetry import TelemetryService, TelemetryMessage
from execution.stream import AlpacaStream

logger = logging.getLogger(__name__)


@dataclass
class PositionSpec:
    symbol: str
    qty: float
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class TradingEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.broker = BrokerClient(settings)
        self.telemetry = TelemetryService(settings)
        self.chaser = PassiveLimitChaser(self.broker, on_filled=self._on_order_filled)
        self.stream: AlpacaStream | None = None
        self._last_stream_event_ts: float | None = None

    def bootstrap(self) -> None:
        # Initialize DB and logging
        configure_logging(self.settings.log_level)
        init_engine(self.settings.database_url)
        create_all()
        logger.info("Engine bootstrapped; rehydrating active positions from DB")
        self.telemetry.start_deadman()

    def rehydrate_positions(self) -> Iterable[str]:
        symbols: list[str] = []
        with session_scope() as s:
            positions = get_all_positions(s)
            for p in positions:
                logger.info("Rehydrated position", extra={
                    "symbol": p.symbol,
                    "qty": p.qty,
                    "entry_price": p.entry_price,
                    "stop_loss": p.stop_loss,
                    "take_profit": p.take_profit,
                    "opened_at": p.opened_at.isoformat(),
                })
                symbols.append(p.symbol)
        return symbols

    def open_position(self, spec: PositionSpec) -> None:
        with session_scope() as s:
            upsert_position(
                s,
                symbol=spec.symbol,
                qty=spec.qty,
                entry_price=spec.entry_price,
                stop_loss=spec.stop_loss,
                take_profit=spec.take_profit,
            )
            logger.info("Position opened", extra={
                "symbol": spec.symbol,
                "qty": spec.qty,
                "entry_price": spec.entry_price,
                "stop_loss": spec.stop_loss,
                "take_profit": spec.take_profit,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            })

    def close_position(self, symbol: str) -> None:
        with session_scope() as s:
            if get_position(s, symbol):
                delete_position(s, symbol)
                logger.info("Position closed", extra={"symbol": symbol})
            else:
                logger.info("Attempted to close non-existent position", extra={"symbol": symbol})

    # -------- Phase 2: Strategy Filters Integration --------
    def _fetch_bars_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        data = self.broker.get_bars(symbol, timeframe=timeframe, limit=limit)
        df = bars_json_to_df(data or {})
        return df

    def _portfolio_close_prices(self, symbols: list[str], limit: int) -> pd.DataFrame:
        frames: Dict[str, pd.Series] = {}
        for sym in symbols:
            df = self._fetch_bars_df(sym, timeframe="1Day", limit=limit)
            if not df.empty:
                frames[sym] = df["close"].rename(sym).astype(float)
        if not frames:
            return pd.DataFrame()
        prices = pd.concat(frames.values(), axis=1).dropna(how="all")
        return prices

    def evaluate_pretrade_filters(self, symbol: str) -> dict:
        now = datetime.now(timezone.utc)
        # Daily and weekly data for the symbol
        daily_limit = max(self.settings.sma_slow * 3, 260)
        weekly_limit = max(self.settings.ema_weeks * 3, 60)

        daily_df = self._fetch_bars_df(symbol, timeframe="1Day", limit=daily_limit)
        weekly_df = self._fetch_bars_df(symbol, timeframe="1Week", limit=weekly_limit)
        if weekly_df.empty and not daily_df.empty:
            weekly_df = resample_weekly(daily_df)

        reasons: dict[str, object] = {}
        allowed = True

        # Volatility Regime Filter
        if self.settings.enable_volatility_filter:
            vrf = VolatilityRegimeFilter(
                atr_window=self.settings.atr_window,
                std_window=self.settings.std_window,
                percentile=self.settings.volatility_percentile,
            )
            vstate = vrf.analyze(daily_df)
            reasons["volatility_state"] = {
                "current_atr": vstate.current_atr,
                "atr_p90": vstate.atr_p90,
                "current_std": vstate.current_std,
                "std_p90": vstate.std_p90,
                "high_volatility": vstate.high_volatility,
            }
            if vstate.high_volatility:
                allowed = False

        # Multi-timeframe Golden Cross Validation
        if self.settings.enable_mtf_validation:
            gcv = validate_golden_cross(
                daily_df=daily_df,
                weekly_df=weekly_df,
                fast=self.settings.sma_fast,
                slow=self.settings.sma_slow,
                ema_weeks=self.settings.ema_weeks,
            )
            reasons["golden_cross_validation"] = {
                "golden_cross": gcv.golden_cross,
                "price_above_21w_ema": gcv.price_above_21w_ema,
                "valid": gcv.valid,
                "last_price": gcv.last_price,
                "last_21w_ema": gcv.last_21w_ema,
            }
            # If a golden cross is present but invalid, then this would block a strategy relying on it.
            if gcv.golden_cross and not gcv.valid:
                allowed = False

        # Correlation Protector (portfolio-level)
        leaders = None
        if self.settings.enable_correlation_protector:
            prices = self._portfolio_close_prices(self.settings.tracked_symbols, limit=max(self.settings.corr_lookback_days * 3, 200))
            if not prices.empty:
                cres = correlation_protector(prices, threshold=self.settings.corr_threshold, lookback=self.settings.corr_lookback_days)
                reasons["correlation"] = {
                    "threshold": self.settings.corr_threshold,
                    "allowed_symbols": sorted(cres.allowed_symbols),
                    "leaders": cres.leaders,
                }
                leaders = cres.allowed_symbols
                if symbol not in cres.allowed_symbols:
                    allowed = False

        # Macro Gatekeeper
        if self.settings.enable_macro_gatekeeper:
            events = parse_events(self.settings.macro_events)
            block, blockers = should_block(now, symbol, events, horizon_hours=self.settings.macro_block_hours)
            reasons["macro_gatekeeper"] = {
                "blocked": block,
                "blockers": [
                    {"type": b.type, "symbol": b.symbol, "start": b.start.isoformat(), "impact": b.impact}
                    for b in blockers
                ],
            }
            if block:
                allowed = False

        reasons["allowed"] = allowed
        return reasons

    # -------- Phase 3: Event-driven lifecycle --------
    def _entry_condition(self, filters: dict) -> bool:
        # Entry condition: golden cross detected AND overall allowed
        gcv = filters.get("golden_cross_validation", {})
        return bool(gcv.get("golden_cross") and filters.get("allowed"))

    def _kelly_size(self, symbol: str, price: float) -> int:
        with session_scope() as s:
            kr = compute_kelly_for_symbol(s, symbol)
        acct = self.broker.get_account() or {}
        equity = float(acct.get("equity", 0.0) or acct.get("cash", 0.0) or 0.0)
        if equity == 0.0 and self.settings.environment_mode == "SHADOW":
            equity = 100000.0  # default paper equity for shadow mode sizing
        risk_capital = max(0.0, kr.kelly_fraction) * equity
        if price <= 0:
            return 0
        qty = int(risk_capital // price)
        return max(qty, 0)

    def _on_bar_event(self, symbol: str, ev: dict) -> None:
        # Update heartbeat and evaluate filters on new bar
        from time import time as _now
        self._last_stream_event_ts = _now()
        self.telemetry.heartbeat()
        try:
            filters = self.evaluate_pretrade_filters(symbol)
            logger.info("Stream event filters", extra={"symbol": symbol, **filters})
            if self._entry_condition(filters):
                # Decide size with Kelly
                quote = self.broker.get_latest_quote(symbol) or {}
                bp = float(((quote.get("quote") or {}).get("bp") or 0.0))
                if bp <= 0:
                    return
                qty = self._kelly_size(symbol, bp)
                if qty <= 0:
                    return
                # Start passively chasing limit at best bid with bracket TP/SL
                self.chaser.start_chase(
                    symbol=symbol,
                    qty=qty,
                    side="buy",
                    take_profit_pct=self.settings.take_profit_pct,
                    stop_loss_pct=self.settings.stop_loss_pct,
                )
                self.telemetry.send(
                    TelemetryMessage(
                        title="Entry Signal Confirmed",
                        description=f"Starting passive bracket chase for {symbol}",
                        fields={
                            "symbol": symbol,
                            "qty": qty,
                            "tp_pct": self.settings.take_profit_pct,
                            "sl_pct": self.settings.stop_loss_pct,
                        },
                    )
                )
        except Exception as e:
            logger.error("on_bar_event error: %s", e, exc_info=True)

    def _on_order_filled(self, symbol: str, order_id: str, limit_price: float, tp_price: float, sl_price: float, qty: float) -> None:
        # Record position in DB and send telemetry
        with session_scope() as s:
            upsert_position(
                s,
                symbol=symbol,
                qty=qty,
                entry_price=limit_price,
                stop_loss=sl_price,
                take_profit=tp_price,
            )
        self.telemetry.send(
            TelemetryMessage(
                title="Order Filled",
                description=f"Bracket entry filled for {symbol}",
                fields={
                    "symbol": symbol,
                    "order_id": order_id,
                    "limit_price": limit_price,
                    "take_profit_price": tp_price,
                    "stop_loss_price": sl_price,
                    "qty": qty,
                },
            )
        )



def run_once() -> None:
    settings = load_settings()
    engine = TradingEngine(settings)
    engine.bootstrap()

    # First lifecycle event: rehydrate existing positions
    engine.rehydrate_positions()

    # Phase 3: Start Alpaca WebSocket stream and process bars in real time
    engine.stream = AlpacaStream(settings, settings.tracked_symbols, engine._on_bar_event)
    engine.stream.start()
    logger.info("Streaming started", extra={"symbols": settings.tracked_symbols, "stream_url": settings.alpaca_stream_url})

    # Keep main thread alive while background threads operate
    import time as _time
    try:
        while True:
            _time.sleep(settings.heartbeat_interval_seconds)
            engine.telemetry.heartbeat()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        if engine.stream:
            engine.stream.stop()
