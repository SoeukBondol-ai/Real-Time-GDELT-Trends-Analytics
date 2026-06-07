"""SQLAlchemy ORM models for the trends analytics database.

These models mirror the tables defined in sql/init.sql and provide
a Pythonic, type-safe way to interact with the database — no raw
SQL needed for CRUD or simple queries.
"""

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Date,
    Double,
    Index,
    Integer,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import TIMESTAMPTZ
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


class TrendWindow(Base):
    __tablename__ = "trend_windows"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    window_start: Mapped[datetime] = mapped_column(TIMESTAMPTZ, primary_key=True, nullable=False)
    window_end: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False)
    keyword: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    mention_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    avg_sentiment: Mapped[float] = mapped_column(Double, nullable=False)
    trend_score: Mapped[float] = mapped_column(Double, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, nullable=False, server_default=text("NOW()")
    )

    __table_args__ = (
        Index("idx_trend_windows_latest", "window_end", "trend_score"),
        Index("idx_trend_windows_keyword_time", "keyword", "window_end"),
    )


class DailyTrendSummary(Base):
    __tablename__ = "daily_trend_summary"

    summary_date: Mapped[date] = mapped_column(Date, primary_key=True, nullable=False)
    keyword: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)
    source: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)
    total_mentions: Mapped[int] = mapped_column(BigInteger, nullable=False)
    avg_sentiment: Mapped[float] = mapped_column(Double, nullable=False)
    max_trend_score: Mapped[float] = mapped_column(Double, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, nullable=False, server_default=text("NOW()")
    )