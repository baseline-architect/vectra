from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, Optional

import pandas as pd


def _parse_time(ts: Any) -> Optional[pd.Timestamp]:
    try:
        return pd.to_datetime(ts, utc=True)
    except Exception:
        return None


def bars_json_to_df(payload: Dict[str, Any]) -> pd.DataFrame:
    # Supports Alpaca Data API v2 style: {"bars": [{"t": iso, "o":...,"h":...,"l":...,"c":...,"v":...}, ...]}
    bars = None
    if isinstance(payload, dict):
        if "bars" in payload and isinstance(payload["bars"], list):
            bars = payload["bars"]
        elif "data" in payload and isinstance(payload["data"], dict) and isinstance(payload["data"].get("bars"), list):
            bars = payload["data"]["bars"]
    if not bars:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])  # empty

    rows = []
    for b in bars:
        t = _parse_time(b.get("t"))
        if t is None:
            continue
        rows.append(
            {
                "time": t,
                "open": float(b.get("o", 0.0)),
                "high": float(b.get("h", 0.0)),
                "low": float(b.get("l", 0.0)),
                "close": float(b.get("c", 0.0)),
                "volume": float(b.get("v", 0.0)),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])  # empty
    df = pd.DataFrame.from_records(rows).set_index("time").sort_index()
    return df


def resample_weekly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # Weekly bar: last close of week, high=max, low=min, open=first, volume=sum
    o = df["open"].resample("W-FRI").first()
    h = df["high"].resample("W-FRI").max()
    l = df["low"].resample("W-FRI").min()
    c = df["close"].resample("W-FRI").last()
    v = df["volume"].resample("W-FRI").sum()
    weekly = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v}).dropna()
    return weekly
