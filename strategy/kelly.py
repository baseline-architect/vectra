from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from models.metrics import StrategyMetric


@dataclass
class KellyResult:
    win_rate: float
    win_loss_ratio: float
    kelly_fraction: float


def kelly_fraction(p: float, b: float) -> float:
    # Kelly Criterion: f* = p - (1 - p) / b
    if b <= 0:
        return 0.0
    f = p - (1 - p) / b
    # Clamp to [0, 1]
    if f < 0:
        return 0.0
    if f > 1:
        return 1.0
    return float(f)


def compute_kelly_for_symbol(session: Session, symbol: str) -> KellyResult:
    m = session.get(StrategyMetric, symbol)
    if not m:
        # Conservative default if no metrics
        return KellyResult(win_rate=0.5, win_loss_ratio=1.0, kelly_fraction=0.0)
    f = kelly_fraction(m.win_rate, m.win_loss_ratio)
    return KellyResult(win_rate=m.win_rate, win_loss_ratio=m.win_loss_ratio, kelly_fraction=f)
