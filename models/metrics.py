from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import String, Float, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class StrategyMetric(Base):
    __tablename__ = "strategy_metrics"

    symbol: Mapped[str] = mapped_column(String(16), primary_key=True)
    win_rate: Mapped[float] = mapped_column(Float, nullable=False)  # p in [0,1]
    win_loss_ratio: Mapped[float] = mapped_column(Float, nullable=False)  # b = avg win / avg loss (>0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    def __repr__(self) -> str:
        return f"StrategyMetric(symbol={self.symbol!r}, win_rate={self.win_rate}, win_loss_ratio={self.win_loss_ratio})"
