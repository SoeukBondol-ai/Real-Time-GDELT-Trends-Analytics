import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class JsonKafkaProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
                "utf-8"
            ),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
            linger_ms=50,
        )
        await self._producer.start()
        logger.info("Kafka producer connected to %s", self.bootstrap_servers)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def send(
        self, topic: str, value: dict[str, Any], key: str | None = None
    ) -> None:
        if not self._producer:
            raise RuntimeError("Producer is not started")
        await self._producer.send_and_wait(topic, value=value, key=key)
