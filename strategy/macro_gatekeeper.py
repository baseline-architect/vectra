from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Tuple


@dataclass(frozen=True)
class MacroEvent:
    type: str  # e.g., FOMC, CPI, EARNINGS
    symbol: str  # "MARKET" or specific ticker
    start: datetime
    impact: str  # e.g., high, medium, low


def parse_events(raw_events: List[dict]) -> List[MacroEvent]:
    events: List[MacroEvent] = []
    for e in raw_events:
        try:
            start = e.get("start")
            if isinstance(start, str):
                dt = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(timezone.utc)
            elif isinstance(start, (int, float)):
                dt = datetime.fromtimestamp(float(start), tz=timezone.utc)
            elif isinstance(start, datetime):
                dt = start.astimezone(timezone.utc)
            else:
                continue
            events.append(
                MacroEvent(
                    type=str(e.get("type", "UNKNOWN")).upper(),
                    symbol=str(e.get("symbol", "MARKET")).upper(),
                    start=dt,
                    impact=str(e.get("impact", "LOW")).lower(),
                )
            )
        except Exception:
            # Ignore malformed entries to keep system robust
            continue
    return events


def should_block(now: datetime, symbol: str, events: Iterable[MacroEvent], horizon_hours: int = 48) -> Tuple[bool, List[MacroEvent]]:
    block_until = now + timedelta(hours=horizon_hours)
    blockers: List[MacroEvent] = []
    for e in events:
        if e.impact != "high":
            continue
        if now <= e.start <= block_until and (e.symbol == "MARKET" or e.symbol == symbol.upper()):
            blockers.append(e)
    return (len(blockers) > 0, blockers)
