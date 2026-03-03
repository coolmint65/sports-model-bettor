"""
Async SQLAlchemy database setup with aiosqlite.

Provides the async engine, session factory, and database initialization
utilities. Uses SQLAlchemy 2.0 async style throughout.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

logger = logging.getLogger(__name__)

# Create the async engine
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    future=True,
    connect_args={"check_same_thread": False},
)

# Create the async session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency that provides an async database session.

    Usage with FastAPI:
        @router.get("/items")
        async def get_items(session: AsyncSession = Depends(get_session)):
            ...

    The session is automatically closed when the request completes.
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_session_context() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager for obtaining an async database session outside of
    FastAPI dependency injection (e.g., in scrapers, scheduled tasks).

    Usage:
        async with get_session_context() as session:
            result = await session.execute(select(Team))
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """
    Initialize the database by creating all tables defined in the models.

    This imports the Base metadata from the models and issues CREATE TABLE
    statements for any tables that do not yet exist. Existing tables are
    left unchanged.
    """
    # Ensure the data directory exists
    settings.db_dir.mkdir(parents=True, exist_ok=True)

    # Import Base so all model metadata is registered
    from app.models.base import Base

    # Import all models to ensure they are registered with Base.metadata
    import app.models.team  # noqa: F401
    import app.models.player  # noqa: F401
    import app.models.game  # noqa: F401
    import app.models.prediction  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Add new columns to existing tables if they don't exist yet (SQLite)
    await _migrate_add_columns()


async def _migrate_add_columns() -> None:
    """Add new columns to existing tables if they are missing (SQLite only)."""
    new_columns = [
        ("game", "home_moneyline", "FLOAT"),
        ("game", "away_moneyline", "FLOAT"),
        ("game", "over_under_line", "FLOAT"),
        ("game", "home_spread_line", "FLOAT"),
        ("game", "away_spread_line", "FLOAT"),
        ("game", "home_spread_price", "FLOAT"),
        ("game", "away_spread_price", "FLOAT"),
        ("game", "over_price", "FLOAT"),
        ("game", "under_price", "FLOAT"),
        ("game", "odds_updated_at", "DATETIME"),
        ("game", "period", "INTEGER"),
        ("game", "period_type", "VARCHAR(10)"),
        ("game", "clock", "VARCHAR(10)"),
        ("game", "clock_running", "BOOLEAN"),
        ("game", "in_intermission", "BOOLEAN"),
        # Prediction phase (prematch vs live)
        ("prediction", "phase", "VARCHAR(20) DEFAULT 'prematch'"),
        # Pregame odds snapshot (frozen when game goes live)
        ("game", "pregame_home_moneyline", "FLOAT"),
        ("game", "pregame_away_moneyline", "FLOAT"),
        ("game", "pregame_over_under_line", "FLOAT"),
        ("game", "pregame_home_spread_line", "FLOAT"),
        ("game", "pregame_away_spread_line", "FLOAT"),
        ("game", "pregame_home_spread_price", "FLOAT"),
        ("game", "pregame_away_spread_price", "FLOAT"),
        ("game", "pregame_over_price", "FLOAT"),
        ("game", "pregame_under_price", "FLOAT"),
        # All available total/spread lines (JSON)
        ("game", "all_total_lines", "JSON"),
        ("game", "all_spread_lines", "JSON"),
        # TrackedBet lock lifecycle
        ("tracked_bet", "locked_at", "DATETIME"),
    ]

    async with engine.begin() as conn:
        for table, column, col_type in new_columns:
            try:
                await conn.execute(
                    text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                )
                logger.info("Added column %s.%s", table, column)
            except Exception:
                # Column already exists
                pass


async def close_db() -> None:
    """Dispose of the database engine and release all connections."""
    await engine.dispose()
