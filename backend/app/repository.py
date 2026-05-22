import json
from datetime import datetime

from app.db import get_pg_pool, get_redis


def _serialize_row(row) -> dict:
    return {
        "keyword": row["keyword"],
        "source": row["source"],
        "mention_count": int(row["mention_count"]),
        "avg_sentiment": float(row["avg_sentiment"]),
        "trend_score": float(row["trend_score"]),
        "window_start": row["window_start"].isoformat(),
        "window_end": row["window_end"].isoformat(),
    }


async def latest_trends(limit: int = 20) -> list[dict]:
    redis = get_redis()
    cache_key = f"latest_trends:{limit}"
    cached = await redis.get(cache_key)
    if cached:
        return json.loads(cached)

    pool = get_pg_pool()
    query = """
        WITH per_source_max AS (
            SELECT source, MAX(window_end) AS max_window_end
            FROM trend_windows
            GROUP BY source
        ), recent AS (
            SELECT
                tw.keyword,
                tw.source,
                SUM(tw.mention_count)::BIGINT AS mention_count,
                AVG(tw.avg_sentiment)::DOUBLE PRECISION AS avg_sentiment,
                SUM(tw.trend_score)::DOUBLE PRECISION AS trend_score,
                MIN(tw.window_start) AS window_start,
                MAX(tw.window_end) AS window_end
            FROM trend_windows tw
            JOIN per_source_max psm ON tw.source = psm.source
            WHERE tw.window_end >= psm.max_window_end - INTERVAL '30 minutes'
            GROUP BY tw.keyword, tw.source
        ), source_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY source ORDER BY trend_score DESC, mention_count DESC) AS srn
            FROM recent
        )
        SELECT keyword, source, mention_count, avg_sentiment, trend_score, window_start, window_end
        FROM source_ranked
        WHERE srn <= $1
        ORDER BY trend_score DESC, mention_count DESC;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, limit)

    payload = [_serialize_row(row) for row in rows]
    await redis.set(cache_key, json.dumps(payload), ex=2)
    return payload


async def trend_history(keyword: str, hours: int = 2) -> list[dict]:
    pool = get_pg_pool()
    query = """
        WITH deduped AS (
            SELECT *
            FROM (
                SELECT
                    tw.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY window_start, window_end, keyword, source
                        ORDER BY batch_id DESC, id DESC
                    ) AS rn
                FROM trend_windows tw
                WHERE keyword = $1
                  AND window_end >= NOW() - ($2::TEXT || ' hours')::INTERVAL
            ) x
            WHERE rn = 1
        )
        SELECT
            time_bucket('10 seconds', window_end) AS bucket,
            keyword,
            source,
            SUM(mention_count)::BIGINT AS mention_count,
            AVG(avg_sentiment)::DOUBLE PRECISION AS avg_sentiment,
            SUM(trend_score)::DOUBLE PRECISION AS trend_score
        FROM deduped
        GROUP BY bucket, keyword, source
        ORDER BY bucket ASC;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, keyword, hours)

    return [
        {
            "bucket": row["bucket"].isoformat(),
            "keyword": row["keyword"],
            "source": row["source"],
            "mention_count": int(row["mention_count"]),
            "avg_sentiment": float(row["avg_sentiment"]),
            "trend_score": float(row["trend_score"]),
        }
        for row in rows
    ]


async def database_ping() -> bool:
    pool = get_pg_pool()
    async with pool.acquire() as conn:
        value = await conn.fetchval("SELECT 1")
    return value == 1


async def redis_ping() -> bool:
    redis = get_redis()
    return bool(await redis.ping())
