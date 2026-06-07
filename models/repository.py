from __future__ import annotations

from typing import Iterable, Optional

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from .position import Position


def get_all_positions(session: Session) -> Iterable[Position]:
    return session.scalars(select(Position)).all()


def get_position(session: Session, symbol: str) -> Optional[Position]:
    return session.get(Position, symbol)


def upsert_position(
    session: Session,
    *,
    symbol: str,
    qty: float,
    entry_price: float,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
) -> Position:
    pos = session.get(Position, symbol)
    if pos is None:
        pos = Position(
            symbol=symbol,
            qty=qty,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
        session.add(pos)
    else:
        pos.qty = qty
        pos.entry_price = entry_price
        pos.stop_loss = stop_loss
        pos.take_profit = take_profit
    session.flush()  # ensure PK present
    return pos


def delete_position(session: Session, symbol: str) -> None:
    session.execute(delete(Position).where(Position.symbol == symbol))
