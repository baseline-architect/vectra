from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .indicators import sma, ema


@dataclass
class GoldenCrossValidation:
    golden_cross: bool
    price_above_21w_ema: bool
    valid: bool
    last_price: float
    last_21w_ema: float


def detect_daily_golden_cross(daily_close: pd.Series, fast: int = 50, slow: int = 200) -> bool:
    if len(daily_close) < slow + 2:
        return False
    f = sma(daily_close, fast)
    s = sma(daily_close, slow)
    # Cross occurred if yesterday f<=s and today f>s
    return bool(f.iloc[-2] <= s.iloc[-2] and f.iloc[-1] > s.iloc[-1])


def compute_21w_ema(weekly_close: pd.Series, weeks: int = 21) -> pd.Series:
    if len(weekly_close) < max(weeks, 2):
        return ema(weekly_close, weeks)
    return ema(weekly_close, weeks)


def validate_golden_cross(daily_df: pd.DataFrame, weekly_df: pd.DataFrame, fast: int = 50, slow: int = 200, ema_weeks: int = 21) -> GoldenCrossValidation:
    if daily_df.empty or weekly_df.empty:
        return GoldenCrossValidation(False, False, False, 0.0, 0.0)
    close_d = daily_df["close"].astype(float)
    close_w = weekly_df["close"].astype(float)
    gc = detect_daily_golden_cross(close_d, fast, slow)
    ema21w = compute_21w_ema(close_w, ema_weeks)
    last_price = float(close_d.iloc[-1])
    last_ema = float(ema21w.iloc[-1]) if not ema21w.empty else 0.0
    above = last_price > last_ema if last_ema != 0.0 else False
    valid = bool(gc and above)
    return GoldenCrossValidation(gc, above, valid, last_price, last_ema)
