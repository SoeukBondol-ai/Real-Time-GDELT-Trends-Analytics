import asyncio
import logging
import signal

from producer.config import settings
from producer.kafka_client import JsonKafkaProducer
from producer.sources import run_bluesky_source, run_gdelt_source, run_hackernews_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    producer = JsonKafkaProducer(settings.kafka_bootstrap_servers)
    await producer.start()

    tasks: list[asyncio.Task] = []
    modes = {m.strip() for m in settings.producer_mode.lower().split(",")}

    if "bluesky" in modes or "all" in modes:
        tasks.append(
            asyncio.create_task(
                run_bluesky_source(producer, settings.bluesky_jetstream_url)
            )
        )
    if "gdelt" in modes or "all" in modes:
        tasks.append(
            asyncio.create_task(
                run_gdelt_source(
                    producer, settings.gdelt_query, settings.gdelt_poll_seconds
                )
            )
        )
    if "hackernews" in modes or "all" in modes:
        tasks.append(
            asyncio.create_task(
                run_hackernews_source(producer, settings.hn_poll_seconds)
            )
        )

    if not tasks:
        raise ValueError(
            f"Unknown PRODUCER_MODE: {settings.producer_mode}. Use comma-separated values like 'bluesky,gdelt'"
        )

    stop_event = asyncio.Event()

    def handle_stop() -> None:
        logger.info("Shutdown requested")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_stop)

    try:
        await stop_event.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
