"""
Database Engine & Session Management

Key design decisions:
- Async SQLAlchemy engine for non-blocking I/O
- Separate read replica session for heavy reconciliation queries
- Short-lived connections: NEVER hold a connection while awaiting external API calls
- PgBouncer-compatible pool settings (transaction pooling mode)
"""
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager
import structlog
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from app.config import settings

logger = structlog.get_logger(__name__)


class Base(DeclarativeBase):
    pass


# Primary database engine
_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker] = None

# Read replica engine (for reconciliation & analytics)
_replica_engine: Optional[AsyncEngine] = None
_replica_session_factory: Optional[async_sessionmaker] = None


def create_engine() -> AsyncEngine:
    """Create async engine with production-grade pool settings."""
    return create_async_engine(
        settings.DATABASE_URL,
        pool_size=settings.DATABASE_POOL_SIZE,
        max_overflow=settings.DATABASE_MAX_OVERFLOW,
        pool_timeout=settings.DATABASE_POOL_TIMEOUT,
        pool_recycle=settings.DATABASE_POOL_RECYCLE,
        pool_pre_ping=True,
        connect_args={"ssl": "require"},
        echo=settings.DEBUG,
    )


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_engine()
    return _engine


def get_session_factory() -> async_sessionmaker:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
    return _session_factory


def get_replica_session_factory() -> async_sessionmaker:
    """Returns replica session factory, falls back to primary if replica not configured."""
    global _replica_engine, _replica_session_factory
    if _replica_session_factory is None:
        replica_url = settings.DATABASE_REPLICA_URL or settings.DATABASE_URL
        _replica_engine = create_async_engine(
            replica_url,
            pool_size=5,
            max_overflow=5,
            pool_timeout=10,
            pool_pre_ping=True,
        )
        _replica_session_factory = async_sessionmaker(
            _replica_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _replica_session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: provides a database session per request."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_replica_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: read-only replica session for analytics/reconciliation."""
    factory = get_replica_session_factory()
    async with factory() as session:
        try:
            yield session
        finally:
            await session.close()


@asynccontextmanager
async def get_db_context() -> AsyncGenerator[AsyncSession, None]:
    """Context manager for non-FastAPI contexts (workers, scripts)."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def close_db():
    """Cleanup database connections on shutdown."""
    global _engine, _replica_engine
    if _engine:
        await _engine.dispose()
    if _replica_engine:
        await _replica_engine.dispose()
    logger.info("Database connections closed")
