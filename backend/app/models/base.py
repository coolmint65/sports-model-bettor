"""
SQLAlchemy declarative base and common mixins.

Provides the Base class for all ORM models and reusable mixins
for common columns like timestamps.
"""

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import DateTime, Integer, Text, func
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    declared_attr,
    mapped_column,
)
from sqlalchemy.types import TypeDecorator


class JSONText(TypeDecorator):
    """JSON stored as TEXT — portable replacement for sa.JSON on SQLite.

    SQLAlchemy's built-in JSON type can trigger ``CompileError: Can't
    generate DDL for NullType()`` on certain Python / SQLite / platform
    combinations (notably Python 3.14 on Windows).  This TypeDecorator
    avoids the issue by rendering as plain TEXT in DDL while providing
    identical automatic ``json.dumps`` / ``json.loads`` round-tripping.
    """

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: Optional[Any], dialect: Any) -> Optional[str]:
        if value is not None:
            return json.dumps(value)
        return value

    def process_result_value(self, value: Optional[str], dialect: Any) -> Optional[Any]:
        if value is not None:
            return json.loads(value)
        return value


class Base(DeclarativeBase):
    """
    Declarative base for all SQLAlchemy models.

    All models should inherit from this class. It automatically
    generates __tablename__ from the class name.
    """

    @declared_attr.directive
    def __tablename__(cls) -> str:
        """
        Generate table name from class name.

        Converts CamelCase to snake_case:
            Team -> team
            TeamStats -> team_stats
            GamePlayerStats -> game_player_stats
        """
        name = cls.__name__
        result = [name[0].lower()]
        for char in name[1:]:
            if char.isupper():
                result.append("_")
                result.append(char.lower())
            else:
                result.append(char)
        return "".join(result)

    # All models get an auto-incrementing integer primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


class TimestampMixin:
    """
    Mixin that adds created_at and updated_at timestamp columns.

    - created_at: Set automatically when the row is first inserted.
    - updated_at: Set automatically on insert and updated on every modification.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
