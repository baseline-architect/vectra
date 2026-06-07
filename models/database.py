from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session


_engine = None
_SessionLocal: sessionmaker[Session] | None = None


class Base(DeclarativeBase):
    pass


def init_engine(db_url: str):
    global _engine, _SessionLocal
    if _engine is None:
        # echo can be toggled via env if needed; off by default
        _engine = create_engine(db_url, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)
    return _engine


def create_all() -> None:
    if _engine is None:
        raise RuntimeError("Engine is not initialized. Call init_engine() first.")
    Base.metadata.create_all(bind=_engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    if _SessionLocal is None:
        raise RuntimeError("SessionLocal is not initialized. Call init_engine() first.")
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
