from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

engine = None
SessionLocal: sessionmaker[Session] | None = None


class Base(DeclarativeBase):
    pass


def init_db(database_url: str) -> sessionmaker[Session]:
    global engine, SessionLocal
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    engine = create_engine(database_url, connect_args=connect_args)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal


def get_db():
    if SessionLocal is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
