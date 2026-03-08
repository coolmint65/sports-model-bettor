"""
Async SQLAlchemy database setup with aiosqlite.

Provides the async engine, session factory, and database initialization
utilities. Uses SQLAlchemy 2.0 async style throughout.
"""

import asyncio
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

# Global write lock — serialises all DB write transactions so that
# concurrent async tasks (odds sync, full data sync, predictions, etc.)
# never collide on SQLite's single-writer constraint.
db_write_lock = asyncio.Lock()

# Create the async engine.
# timeout=30 raises the SQLite busy-wait from 5 s to 30 s so the
# background sync and API requests don't clash with "database is locked".
#
# Pool sizing: the app has several concurrent DB consumers:
#   - Multiple FastAPI request handlers (schedule, best-bets, tracked bets)
#   - Background scheduler tasks (odds sync, prediction regen, settlement)
#   - Auto-track POST bursts from the frontend (up to ~3 concurrent)
# The default pool_size=5 + max_overflow=10 exhausts under load.
# Raise to pool_size=10 + max_overflow=20 to accommodate bursts.
engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_size=10,
    max_overflow=20,
    pool_timeout=60,
    pool_recycle=3600,
    connect_args={"check_same_thread": False, "timeout": 30},
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


@asynccontextmanager
async def get_write_session_context() -> AsyncGenerator[AsyncSession, None]:
    """Session context that holds the global write lock for the entire
    transaction lifetime.

    Use this for operations that call ``session.flush()`` or execute
    INSERT/UPDATE/DELETE statements, so that they cannot overlap with
    other writers and trigger SQLite "database is locked" errors.

    Usage:
        async with get_write_session_context() as session:
            session.add(obj)
            await session.flush()   # safe — write lock already held
    """
    async with db_write_lock:
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
    import app.models.injury  # noqa: F401
    import app.models.matchup  # noqa: F401

    async with engine.begin() as conn:
        # Enable WAL mode so readers don't block on the background sync writer.
        await conn.execute(text("PRAGMA journal_mode=WAL"))
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
        # Prop odds
        ("game", "btts_yes_price", "FLOAT"),
        ("game", "btts_no_price", "FLOAT"),
        ("game", "first_goal_home_price", "FLOAT"),
        ("game", "first_goal_away_price", "FLOAT"),
        ("game", "overtime_yes_price", "FLOAT"),
        ("game", "overtime_no_price", "FLOAT"),
        ("game", "total_odd_price", "FLOAT"),
        ("game", "total_even_price", "FLOAT"),
        ("game", "period1_total_line", "FLOAT"),
        ("game", "period1_over_price", "FLOAT"),
        ("game", "period1_under_price", "FLOAT"),
        ("game", "period1_home_ml", "FLOAT"),
        ("game", "period1_away_ml", "FLOAT"),
        ("game", "period1_draw_price", "FLOAT"),
        # New prop odds (batch 2)
        ("game", "period1_btts_yes_price", "FLOAT"),
        ("game", "period1_btts_no_price", "FLOAT"),
        ("game", "period1_spread_line", "FLOAT"),
        ("game", "period1_home_spread_price", "FLOAT"),
        ("game", "period1_away_spread_price", "FLOAT"),
        ("game", "regulation_home_price", "FLOAT"),
        ("game", "regulation_away_price", "FLOAT"),
        ("game", "regulation_draw_price", "FLOAT"),
        ("game", "home_team_total_line", "FLOAT"),
        ("game", "home_team_over_price", "FLOAT"),
        ("game", "home_team_under_price", "FLOAT"),
        ("game", "away_team_total_line", "FLOAT"),
        ("game", "away_team_over_price", "FLOAT"),
        ("game", "away_team_under_price", "FLOAT"),
        ("game", "highest_period_p1_price", "FLOAT"),
        ("game", "highest_period_p2_price", "FLOAT"),
        ("game", "highest_period_p3_price", "FLOAT"),
        ("game", "highest_period_tie_price", "FLOAT"),
        # Period 2 odds
        ("game", "period2_total_line", "FLOAT"),
        ("game", "period2_over_price", "FLOAT"),
        ("game", "period2_under_price", "FLOAT"),
        ("game", "period2_home_ml", "FLOAT"),
        ("game", "period2_away_ml", "FLOAT"),
        ("game", "period2_draw_price", "FLOAT"),
        ("game", "period2_spread_line", "FLOAT"),
        ("game", "period2_home_spread_price", "FLOAT"),
        ("game", "period2_away_spread_price", "FLOAT"),
        # Period 3 odds
        ("game", "period3_total_line", "FLOAT"),
        ("game", "period3_over_price", "FLOAT"),
        ("game", "period3_under_price", "FLOAT"),
        ("game", "period3_home_ml", "FLOAT"),
        ("game", "period3_away_ml", "FLOAT"),
        ("game", "period3_draw_price", "FLOAT"),
        ("game", "period3_spread_line", "FLOAT"),
        ("game", "period3_home_spread_price", "FLOAT"),
        ("game", "period3_away_spread_price", "FLOAT"),
        # Game closing line snapshots
        ("game", "closing_home_moneyline", "FLOAT"),
        ("game", "closing_away_moneyline", "FLOAT"),
        ("game", "closing_over_under_line", "FLOAT"),
        ("game", "closing_over_price", "FLOAT"),
        ("game", "closing_under_price", "FLOAT"),
        ("game", "closing_home_spread_line", "FLOAT"),
        ("game", "closing_home_spread_price", "FLOAT"),
        ("game", "closing_away_spread_price", "FLOAT"),
        # BetResult CLV tracking
        ("bet_result", "closing_implied_prob", "FLOAT"),
        ("bet_result", "clv", "FLOAT"),
    ]

    async with engine.begin() as conn:
        for table, column, col_type in new_columns:
            try:
                await conn.execute(
                    text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                )
                logger.info("Added column %s.%s", table, column)
            except Exception:  # noqa: BLE001 — OperationalError when column exists
                pass


async def close_db() -> None:
    """Dispose of the database engine and release all connections."""
    await engine.dispose()
