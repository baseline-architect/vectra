from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .indicators import atr, rolling_std


@dataclass
class VolatilityState:
    current_atr: float
    atr_p90: float
    current_std: float
    std_p90: float
    high_volatility: bool


class VolatilityRegimeFilter:
    def __init__(self, atr_window: int = 20, std_window: int = 20, percentile: float = 0.9) -> None:
        self.atr_window = atr_window
        self.std_window = std_window
        self.percentile = percentile

    def analyze(self, df: pd.DataFrame) -> VolatilityState:
        if df.empty or len(df) < max(self.atr_window, self.std_window) + 1:
            # Not enough data to compute reliable metrics; treat as not high volatility
            return VolatilityState(0.0, 0.0, 0.0, 0.0, False)
        atr_series = atr(df, self.atr_window).dropna()
        std_series = rolling_std(df["close"], self.std_window).dropna()
        if atr_series.empty or std_series.empty:
            return VolatilityState(0.0, 0.0, 0.0, 0.0, False)
        p90_atr = float(atr_series.quantile(self.percentile))
        p90_std = float(std_series.quantile(self.percentile))
        current_atr = float(atr_series.iloc[-1])
        current_std = float(std_series.iloc[-1])
        high = current_atr >= p90_atr or current_std >= p90_std
        return VolatilityState(
            current_atr=current_atr,
            atr_p90=p90_atr,
            current_std=current_std,
            std_p90=p90_std,
            high_volatility=high,
        )
