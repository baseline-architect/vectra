from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from enum import Enum
from pathlib import Path

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

from pydantic import BaseModel, Field, validator, SecretStr, constr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class EnvironmentMode(str, Enum):
    LIVE = "LIVE"
    SHADOW = "SHADOW"
    SANDBOX = "SANDBOX"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class SecureConfig(BaseSettings):
    """
    Secure configuration schema with encrypted memory boundaries.
    Uses Pydantic for runtime validation and type safety.
    Sensitive fields are protected using SecretStr and encryption.
    """
    
    database_url: str = Field(
        default="sqlite:///alpaca_bot.db",
        description="Database connection URL",
    )
    
    alpaca_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="Alpaca API key ID",
    )
    
    alpaca_api_secret: SecretStr = Field(
        default=SecretStr(""),
        description="Alpaca API secret key",
    )
    
    alpaca_trading_base_url: str = Field(
        default="https://paper-api.alpaca.markets",
        description="Alpaca trading API base URL",
    )
    
    alpaca_data_base_url: str = Field(
        default="https://data.alpaca.markets",
        description="Alpaca data API base URL",
    )
    
    alpaca_stream_url: str = Field(
        default="wss://stream.data.alpaca.markets/v2/iex",
        description="Alpaca WebSocket stream URL",
    )
    
    environment_mode: EnvironmentMode = Field(
        default=EnvironmentMode.SHADOW,
        description="Operating environment mode",
    )
    
    log_level: LogLevel = Field(
        default=LogLevel.INFO,
        description="Logging level",
    )
    
    tracked_symbols: List[constr(max_length=10, min_length=1)] = Field(
        default_factory=lambda: ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"],
        description="List of symbols to track",
    )
    
    enable_volatility_filter: bool = Field(
        default=True,
        description="Enable volatility regime filter",
    )
    
    enable_mtf_validation: bool = Field(
        default=True,
        description="Enable multi-timeframe validation",
    )
    
    enable_correlation_protector: bool = Field(
        default=True,
        description="Enable correlation protector",
    )
    
    enable_macro_gatekeeper: bool = Field(
        default=True,
        description="Enable macro gatekeeper",
    )
    
    atr_window: int = Field(
        default=20,
        ge=5,
        le=100,
        description="ATR calculation window",
    )
    
    std_window: int = Field(
        default=20,
        ge=5,
        le=100,
        description="Standard deviation window",
    )
    
    volatility_percentile: float = Field(
        default=0.9,
        ge=0.5,
        le=1.0,
        description="Volatility percentile threshold",
    )
    
    sma_fast: int = Field(
        default=50,
        ge=10,
        le=200,
        description="Fast SMA window",
    )
    
    sma_slow: int = Field(
        default=200,
        ge=50,
        le=500,
        description="Slow SMA window",
    )
    
    ema_weeks: int = Field(
        default=21,
        ge=5,
        le=52,
        description="EMA weeks for weekly timeframe",
    )
    
    corr_threshold: float = Field(
        default=0.75,
        ge=0.0,
        le=1.0,
        description="Correlation threshold",
    )
    
    corr_lookback_days: int = Field(
        default=60,
        ge=10,
        le=365,
        description="Correlation lookback period in days",
    )
    
    momentum_lookback_days: int = Field(
        default=60,
        ge=10,
        le=365,
        description="Momentum lookback period in days",
    )
    
    macro_block_hours: int = Field(
        default=48,
        ge=1,
        le=168,
        description="Hours before/after macro events to block trading",
    )
    
    macro_events: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Macro economic events schedule",
    )
    
    take_profit_pct: float = Field(
        default=0.08,
        ge=0.01,
        le=1.0,
        description="Take profit percentage",
    )
    
    stop_loss_pct: float = Field(
        default=0.025,
        ge=0.005,
        le=0.5,
        description="Stop loss percentage",
    )
    
    telemetry_webhook_urls: List[str] = Field(
        default_factory=list,
        description="Telemetry webhook URLs",
    )
    
    healthcheck_url: Optional[str] = Field(
        default=None,
        description="Healthcheck URL for deadman switch",
    )
    
    heartbeat_interval_seconds: int = Field(
        default=60,
        ge=10,
        le=600,
        description="Heartbeat interval in seconds",
    )
    
    deadman_timeout_seconds: int = Field(
        default=300,
        ge=60,
        le=3600,
        description="Deadman switch timeout in seconds",
    )
    
    max_daily_drawdown_pct: float = Field(
        default=0.15,
        ge=0.01,
        le=0.50,
        description="Maximum daily drawdown percentage",
    )
    
    max_consecutive_trades: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum consecutive trades before throttle",
    )
    
    max_trades_per_minute: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum trades per minute",
    )
    
    max_price_change_pct: float = Field(
        default=0.20,
        ge=0.05,
        le=1.0,
        description="Maximum acceptable price change percentage",
    )
    
    max_position_value_pct: float = Field(
        default=0.30,
        ge=0.05,
        le=0.95,
        description="Maximum position value as percentage of equity",
    )
    
    circuit_breaker_cooldown_seconds: float = Field(
        default=300.0,
        ge=60.0,
        le=3600.0,
        description="Circuit breaker cooldown period in seconds",
    )
    
    rate_limit_trading_capacity: int = Field(
        default=200,
        ge=50,
        le=1000,
        description="Trading API rate limit capacity",
    )
    
    rate_limit_data_capacity: int = Field(
        default=200,
        ge=50,
        le=1000,
        description="Data API rate limit capacity",
    )
    
    rate_limit_order_capacity: int = Field(
        default=50,
        ge=10,
        le=200,
        description="Order submission rate limit capacity",
    )
    
    cache_max_size: int = Field(
        default=10000,
        ge=1000,
        le=100000,
        description="Maximum cache size",
    )
    
    cache_default_ttl: float = Field(
        default=300.0,
        ge=10.0,
        le=3600.0,
        description="Default cache TTL in seconds",
    )
    
    db_batch_size: int = Field(
        default=50,
        ge=10,
        le=500,
        description="Database batch write size",
    )
    
    db_flush_interval: float = Field(
        default=1.0,
        ge=0.1,
        le=10.0,
        description="Database flush interval in seconds",
    )
    
    websocket_ping_interval: float = Field(
        default=20.0,
        ge=5.0,
        le=60.0,
        description="WebSocket ping interval in seconds",
    )
    
    websocket_ping_timeout: float = Field(
        default=10.0,
        ge=2.0,
        le=30.0,
        description="WebSocket ping timeout in seconds",
    )
    
    spread_tolerance_pct: float = Field(
        default=0.001,
        ge=0.0001,
        le=0.01,
        description="Spread tolerance percentage for order placement",
    )
    
    max_slippage_pct: float = Field(
        default=0.005,
        ge=0.001,
        le=0.05,
        description="Maximum acceptable slippage percentage",
    )
    
    @validator("sma_slow")
    def validate_sma_windows(cls, v, values):
        if "sma_fast" in values and v <= values["sma_fast"]:
            raise ValueError("sma_slow must be greater than sma_fast")
        return v
    
    @validator("tracked_symbols")
    def validate_symbols(cls, v):
        if not v:
            raise ValueError("tracked_symbols cannot be empty")
        unique_symbols = set(s.upper() for s in v)
        if len(unique_symbols) != len(v):
            raise ValueError("tracked_symbols must contain unique symbols")
        return list(unique_symbols)
    
    @validator("macro_events")
    def validate_macro_events(cls, v):
        for event in v:
            if not isinstance(event, dict):
                raise ValueError("macro_events must be a list of dictionaries")
            required_fields = {"type", "symbol", "start", "impact"}
            if not required_fields.issubset(event.keys()):
                raise ValueError(f"macro event missing required fields: {required_fields}")
        return v
    
    @validator("telemetry_webhook_urls")
    def validate_webhook_urls(cls, v):
        for url in v:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"Invalid webhook URL: {url}")
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "forbid"


class ConfigManager:
    """
    Secure configuration manager with encryption support.
    Provides sandboxed configuration injection with encrypted memory boundaries.
    """
    
    def __init__(self, config_path: Optional[Path] = None, encryption_key: Optional[bytes] = None) -> None:
        self._config_path = config_path or Path("config/secure_config.json")
        self._encryption_key = encryption_key
        self._config: Optional[SecureConfig] = None
        self._fernet: Optional[Fernet] = None
        self._lock = __import__("threading").RLLock()
        
        if self._encryption_key:
            self._fernet = Fernet(self._encryption_key)
        
        self._load_config()
    
    def _load_config(self) -> None:
        try:
            self._config = SecureConfig()
            logger.info("Secure configuration loaded", extra={
                "environment_mode": self._config.environment_mode.value,
                "tracked_symbols": self._config.tracked_symbols,
            })
        except Exception as e:
            logger.error("Failed to load secure configuration", exc_info=True)
            raise
    
    def get_config(self) -> SecureConfig:
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        return self._config
    
    def get_sensitive_value(self, field_name: str) -> str:
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        
        field_value = getattr(self._config, field_name, None)
        if isinstance(field_value, SecretStr):
            return field_value.get_secret_value()
        return str(field_value)
    
    def encrypt_value(self, value: str) -> str:
        if self._fernet is None:
            raise RuntimeError("Encryption not enabled")
        encrypted = self._fernet.encrypt(value.encode())
        return base64.urlsafe_b64encode(encrypted).decode()
    
    def decrypt_value(self, encrypted_value: str) -> str:
        if self._fernet is None:
            raise RuntimeError("Encryption not enabled")
        encrypted = base64.urlsafe_b64decode(encrypted_value.encode())
        decrypted = self._fernet.decrypt(encrypted)
        return decrypted.decode()
    
    def save_encrypted_config(self, output_path: Path) -> None:
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        
        config_dict = self._config.dict()
        
        encrypted_dict = {}
        for key, value in config_dict.items():
            if isinstance(value, SecretStr):
                encrypted_dict[key] = self.encrypt_value(value.get_secret_value())
            elif isinstance(value, list) and all(isinstance(item, SecretStr) for item in value):
                encrypted_dict[key] = [self.encrypt_value(item.get_secret_value()) for item in value]
            else:
                encrypted_dict[key] = value
        
        with open(output_path, "w") as f:
            json.dump(encrypted_dict, f, indent=2, default=str)
        
        logger.info("Encrypted configuration saved", extra={"path": str(output_path)})
    
    @staticmethod
    def generate_encryption_key(password: str, salt: Optional[bytes] = None) -> bytes:
        if salt is None:
            salt = os.urandom(16)
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key
    
    def reload_config(self) -> None:
        with self._lock:
            self._load_config()
            logger.info("Configuration reloaded")
    
    def validate_config(self) -> Dict[str, Any]:
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        
        validation_results = {
            "valid": True,
            "errors": [],
            "warnings": [],
        }
        
        if self._config.environment_mode == EnvironmentMode.LIVE:
            if not self._config.alpaca_api_key.get_secret_value():
                validation_results["errors"].append("API key required for LIVE mode")
            if not self._config.alpaca_api_secret.get_secret_value():
                validation_results["errors"].append("API secret required for LIVE mode")
            
            if self._config.max_daily_drawdown_pct > 0.25:
                validation_results["warnings"].append("High daily drawdown threshold for LIVE mode")
        
        if self._config.take_profit_pct < self._config.stop_loss_pct:
            validation_results["errors"].append("Take profit must be greater than stop loss")
        
        if len(validation_results["errors"]) > 0:
            validation_results["valid"] = False
        
        return validation_results
    
    def get_config_hash(self) -> str:
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        
        import hashlib
        config_str = json.dumps(self._config.dict(), sort_keys=True, default=str)
        return hashlib.sha256(config_str.encode()).hexdigest()
