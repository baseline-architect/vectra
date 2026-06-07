from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Float, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class Position(Base):
    __tablename__ = "active_positions"

    symbol: Mapped[str] = mapped_column(String(16), primary_key=True)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    stop_loss: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    take_profit: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    def __repr__(self) -> str:  # useful for diagnostics
        return (
            f"Position(symbol={self.symbol!r}, qty={self.qty}, entry_price={self.entry_price}, "
            f"stop_loss={self.stop_loss}, take_profit={self.take_profit}, opened_at={self.opened_at.isoformat()})"
        )
