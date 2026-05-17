from datetime import datetime

from pydantic import BaseModel


class TrendItem(BaseModel):
    keyword: str
    source: str
    mention_count: int
    avg_sentiment: float
    trend_score: float
    window_start: datetime
    window_end: datetime


class HealthResponse(BaseModel):
    status: str
    database: str
    redis: str
