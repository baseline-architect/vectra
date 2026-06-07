import json
import os
from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass(frozen=True)
class Settings:
    database_url: str
    alpaca_api_key: Optional[str]
    alpaca_api_secret: Optional[str]
    alpaca_trading_base_url: str
    alpaca_data_base_url: str
    log_level: str
    # Phase 2 strategy controls
    tracked_symbols: List[str] = field(default_factory=lambda: ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"])
    enable_volatility_filter: bool = True
    enable_mtf_validation: bool = True
    enable_correlation_protector: bool = True
    enable_macro_gatekeeper: bool = True
    # Volatility regime
    atr_window: int = 20
    std_window: int = 20
    volatility_percentile: float = 0.9
    # Multi-timeframe
    sma_fast: int = 50
    sma_slow: int = 200
    ema_weeks: int = 21
    # Correlation protector
    corr_threshold: float = 0.75
    corr_lookback_days: int = 60
    momentum_lookback_days: int = 60
    # Macro gatekeeper
    macro_block_hours: int = 48
    macro_events: List[dict] = field(default_factory=list)
    # Phase 3 execution & telemetry
    environment_mode: str = "SHADOW"  # LIVE | SHADOW
    take_profit_pct: float = 0.08
    stop_loss_pct: float = 0.025
    alpaca_stream_url: str = "wss://stream.data.alpaca.markets/v2/iex"
    telemetry_webhook_urls: List[str] = field(default_factory=list)
    healthcheck_url: Optional[str] = None
    heartbeat_interval_seconds: int = 60
    deadman_timeout_seconds: int = 300


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_list(value: Optional[str], default: List[str]) -> List[str]:
    if not value:
        return default
    return [x.strip().upper() for x in value.split(",") if x.strip()]


def _parse_json_list(value: Optional[str], default: List[dict]) -> List[dict]:
    if not value:
        return default
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
        return default
    except json.JSONDecodeError:
        return default


def _parse_str_list(value: Optional[str], default: List[str]) -> List[str]:
    if not value:
        return default
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except json.JSONDecodeError:
        pass
    # Fallback CSV
    return [x.strip() for x in value.split(",") if x.strip()]


def load_settings() -> Settings:
    # Defaults are safe for local use. DATABASE_URL is SQLite in project root by default.
    db_url = os.getenv("DATABASE_URL", "sqlite:///alpaca_bot.db")
    trading_base = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
    data_base = os.getenv("APCA_DATA_BASE_URL", "https://data.alpaca.markets")
    tracked = _parse_list(os.getenv("TRACKED_SYMBOLS"), ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"])
    macro_events = _parse_json_list(os.getenv("MACRO_EVENTS_JSON"), [])
    env_mode = (os.getenv("ENVIRONMENT_MODE", "SHADOW").strip().upper())
    if env_mode not in {"LIVE", "SHADOW"}:
        env_mode = "SHADOW"
    telemetry_urls = _parse_str_list(os.getenv("TELEMETRY_WEBHOOK_URLS"), [])
    healthcheck_url = os.getenv("HEALTHCHECK_URL")

    return Settings(
        database_url=db_url,
        alpaca_api_key=os.getenv("APCA_API_KEY_ID"),
        alpaca_api_secret=os.getenv("APCA_API_SECRET_KEY"),
        alpaca_trading_base_url=trading_base.rstrip("/"),
        alpaca_data_base_url=data_base.rstrip("/"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        tracked_symbols=tracked,
        enable_volatility_filter=_parse_bool(os.getenv("ENABLE_VOLATILITY_FILTER"), True),
        enable_mtf_validation=_parse_bool(os.getenv("ENABLE_MTF_VALIDATION"), True),
        enable_correlation_protector=_parse_bool(os.getenv("ENABLE_CORRELATION_PROTECTOR"), True),
        enable_macro_gatekeeper=_parse_bool(os.getenv("ENABLE_MACRO_GATEKEEPER"), True),
        atr_window=int(os.getenv("ATR_WINDOW", "20")),
        std_window=int(os.getenv("STD_WINDOW", "20")),
        volatility_percentile=float(os.getenv("VOLATILITY_PERCENTILE", "0.9")),
        sma_fast=int(os.getenv("SMA_FAST", "50")),
        sma_slow=int(os.getenv("SMA_SLOW", "200")),
        ema_weeks=int(os.getenv("EMA_WEEKS", "21")),
        corr_threshold=float(os.getenv("CORR_THRESHOLD", "0.75")),
        corr_lookback_days=int(os.getenv("CORR_LOOKBACK_DAYS", "60")),
        momentum_lookback_days=int(os.getenv("MOMENTUM_LOOKBACK_DAYS", "60")),
        macro_block_hours=int(os.getenv("MACRO_BLOCK_HOURS", "48")),
        macro_events=macro_events,
        environment_mode=env_mode,
        take_profit_pct=float(os.getenv("TAKE_PROFIT_PCT", "0.08")),
        stop_loss_pct=float(os.getenv("STOP_LOSS_PCT", "0.025")),
        alpaca_stream_url=os.getenv("ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets/v2/iex"),
        telemetry_webhook_urls=telemetry_urls,
        healthcheck_url=healthcheck_url,
        heartbeat_interval_seconds=int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "60")),
        deadman_timeout_seconds=int(os.getenv("DEADMAN_TIMEOUT_SECONDS", "300")),
    )
