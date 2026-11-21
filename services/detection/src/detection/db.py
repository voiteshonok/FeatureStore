"""Async SQLAlchemy engine for the alert CRUD API (ADR-019)."""

from urllib.parse import quote_plus

from psycopg.conninfo import conninfo_to_dict
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def as_sqlalchemy_url(database_url: str) -> str:
    """Map a libpq URL or keyword DSN to the SQLAlchemy psycopg dialect."""
    if database_url.startswith("postgresql+psycopg://"):
        return database_url
    if database_url.startswith("postgresql://"):
        return "postgresql+psycopg://" + database_url.removeprefix("postgresql://")
    if database_url.startswith("postgres://"):
        return "postgresql+psycopg://" + database_url.removeprefix("postgres://")
    # Keyword/value conninfo from testcontainers / psycopg.make_conninfo.
    info = conninfo_to_dict(database_url)
    user = quote_plus(str(info.get("user") or ""))
    password = quote_plus(str(info.get("password") or ""))
    host = str(info.get("host") or "localhost")
    port = str(info.get("port") or "5432")
    dbname = str(info.get("dbname") or "")
    auth = f"{user}:{password}@" if password else (f"{user}@" if user else "")
    return f"postgresql+psycopg://{auth}{host}:{port}/{dbname}"


def make_engine(database_url: str) -> AsyncEngine:
    """Build an async engine. Pool size matches the consumer pool."""
    return create_async_engine(
        as_sqlalchemy_url(database_url),
        pool_size=4,
        max_overflow=0,
        pool_pre_ping=True,
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory; callers open short-lived sessions per request."""
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
