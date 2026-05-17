import asyncpg
from redis.asyncio import Redis

from app.config import settings

pg_pool: asyncpg.Pool | None = None
redis_client: Redis | None = None


async def connect() -> None:
    global pg_pool, redis_client
    pg_pool = await asyncpg.create_pool(dsn=settings.database_url, min_size=1, max_size=5)
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)


async def disconnect() -> None:
    global pg_pool, redis_client
    if pg_pool:
        await pg_pool.close()
    if redis_client:
        await redis_client.aclose()


def get_pg_pool() -> asyncpg.Pool:
    if pg_pool is None:
        raise RuntimeError("PostgreSQL pool is not initialized")
    return pg_pool


def get_redis() -> Redis:
    if redis_client is None:
        raise RuntimeError("Redis client is not initialized")
    return redis_client
