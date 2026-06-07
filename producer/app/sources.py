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
                        created_at = record.get("createdAt") or utc_now_iso()
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
    seen_urls: set[str] = set()
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"
    backoff = poll_seconds

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

            for article in data.get("articles", []):
                url = article.get("url")
                title = article.get("title") or ""
                if not url or url in seen_urls or not title:
                    continue
                seen_urls.add(url)
                # keep set from growing forever
                if len(seen_urls) > 5000:
                    seen_urls.clear()
                event = {
                    "id": stable_id(url),
                    "source": "gdelt",
                    "author": article.get("domain") or "gdelt",
                    "text": title,
                    "lang": article.get("language") or "unknown",
                    "created_at": article.get("seendate") or utc_now_iso(),
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
