import asyncio
import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import websockets

from app.kafka_client import JsonKafkaProducer

logger = logging.getLogger(__name__)

RAW_SOCIAL_TOPIC = "raw-social-posts"
RAW_NEWS_TOPIC = "raw-news-events"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_timestamp(ts: str | None, max_age_days: int = 365) -> str:
    """Validate a timestamp string. Returns it if it's recent (within max_age_days),
    otherwise returns the current time. Prevents ancient or far-future timestamps
    from breaking Spark's watermarking."""
    if not ts:
        return utc_now_iso()
    ts = ts.strip()
    try:
        # Handle both ISO 8601 and GDELT formats
        if "-" in ts and ":" in ts:
            # ISO 8601: 2026-06-07T05:01:43.144321+00:00 or 2026-06-07T05:01:43Z
            parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            # Might be GDELT format, try normalize_gdelt_date first
            parsed = datetime.fromisoformat(
                normalize_gdelt_date(ts).replace("Z", "+00:00")
            )
        # Check if timestamp is reasonably recent
        now = datetime.now(timezone.utc)
        age = abs((now - parsed).total_seconds())
        if age > max_age_days * 86400:  # Too old or too far in the future
            return utc_now_iso()
        return ts
    except (ValueError, IndexError):
        return utc_now_iso()


def normalize_gdelt_date(raw: str | None) -> str:
    """Convert GDELT seendate like '20260607T044500Z' to ISO 8601 '2026-06-07T04:45:00Z'."""
    if not raw:
        return utc_now_iso()
    raw = raw.strip()
    # Already looks like ISO 8601 with hyphens/colons
    if "-" in raw and ":" in raw:
        return raw
    try:
        # GDELT format: YYYYMMDDTHHMMSSz  (e.g. 20260607T044500Z)
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}T{raw[9:11]}:{raw[11:13]}:{raw[13:15]}Z"
    except (IndexError, ValueError):
        return utc_now_iso()


def stable_id(text: str) -> str:
    payload = f"{text}-{time.time_ns()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


async def run_bluesky_source(producer: JsonKafkaProducer, url: str) -> None:
    logger.info("Starting Bluesky Jetstream source")
    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=60,
                max_size=2_000_000,
                open_timeout=30,
            ) as ws:
                async for message in ws:
                    try:
                        import json

                        payload = json.loads(message)
                        commit = payload.get("commit") or {}
                        record = commit.get("record") or {}
                        text = record.get("text")
                        if not text:
                            continue
                        operation = commit.get("operation")
                        collection = commit.get("collection")
                        if operation != "create" or collection != "app.bsky.feed.post":
                            continue

                        did = payload.get("did", "unknown")
                        created_at = sanitize_timestamp(record.get("createdAt"))
                        event = {
                            "id": stable_id(f"{did}-{created_at}-{text}"),
                            "source": "bluesky",
                            "author": did,
                            "text": text,
                            "lang": "unknown",
                            "created_at": created_at,
                            "url": None,
                            "metadata": {"collection": collection},
                        }
                        await producer.send(RAW_SOCIAL_TOPIC, event, key=event["id"])
                    except Exception:
                        logger.exception("Failed to parse Bluesky message")
        except Exception:
            logger.exception("Bluesky stream disconnected; retrying in 10 seconds")
            await asyncio.sleep(10)


async def run_gdelt_source(
    producer: JsonKafkaProducer, query: str, poll_seconds: int
) -> None:
    logger.info("Starting GDELT source")
    seen_urls: dict[str, float] = {}  # url -> timestamp for bounded dedup
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"
    backoff = poll_seconds
    max_dedup = 10_000

    while True:
        try:
            params = {
                "query": query,
                "mode": "artlist",
                "format": "json",
                "maxrecords": "50",
                "sort": "datedesc",
            }
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(endpoint, params=params)
                if response.status_code == 429:
                    logger.warning("GDELT rate limited, backing off %ds", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 300)
                    continue
                response.raise_for_status()
                backoff = poll_seconds  # reset on success
                data: dict[str, Any] = response.json()

            articles = data.get("articles", [])
            logger.info("GDELT returned %d articles (seen: %d)", len(articles), len(seen_urls))
            for article in articles:
                url = article.get("url")
                title = article.get("title") or ""
                if not url or url in seen_urls or not title:
                    continue
                seen_urls[url] = time.time()
                # Evict oldest entries to keep set bounded
                if len(seen_urls) > max_dedup:
                    oldest = sorted(seen_urls, key=seen_urls.get)[: len(seen_urls) // 2]
                    for k in oldest:
                        del seen_urls[k]
                event = {
                    "id": stable_id(url),
                    "source": "gdelt",
                    "author": article.get("domain") or "gdelt",
                    "text": title,
                    "lang": article.get("language") or "unknown",
                    "created_at": normalize_gdelt_date(article.get("seendate")),
                    "url": url,
                    "metadata": {
                        "domain": article.get("domain"),
                        "source_country": article.get("sourcecountry"),
                    },
                }
                await producer.send(RAW_NEWS_TOPIC, event, key=event["id"])
                logger.info("gdelt event sent: %s", title[:120])
        except Exception:
            logger.exception("GDELT polling failed")
            backoff = min(backoff * 2, 300)

        await asyncio.sleep(poll_seconds)


async def run_hackernews_source(producer: JsonKafkaProducer, poll_seconds: int) -> None:
    logger.info("Starting HackerNews source")
    endpoint = "https://hacker-news.firebaseio.com/v0/newstories.json"

    while True:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                story_ids = response.json()[:50]

                async def fetch_and_send(story_id: int) -> None:
                    try:
                        item_url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
                        item_response = await client.get(item_url)
                        item_response.raise_for_status()
                        story = item_response.json()
                        title = story.get("title")
                        url = story.get("url")
                        if not title:
                            return
                        event = {
                            "id": f"hn_{story_id}",
                            "source": "hackernews",
                            "author": story.get("by") or "hn",
                            "text": title,
                            "lang": "en",
                            "created_at": utc_now_iso(),
                            "url": url
                            or f"https://news.ycombinator.com/item?id={story_id}",
                            "metadata": {
                                "score": story.get("score"),
                                "descendants": story.get("descendants"),
                            },
                        }
                        await producer.send(RAW_NEWS_TOPIC, event, key=event["id"])
                        logger.info("HN event sent: %s", title[:120])
                    except Exception:
                        logger.exception("HN fetch failed for %d", story_id)

                # fetch top 20 concurrently every poll
                await asyncio.gather(*[fetch_and_send(sid) for sid in story_ids[:20]])

        except Exception:
            logger.exception("HackerNews polling failed")

        await asyncio.sleep(poll_seconds)
