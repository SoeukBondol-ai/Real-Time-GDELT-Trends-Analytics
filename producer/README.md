# Producer service

This service sends raw social/news events into Kafka.

Supported modes:

- mock: local fake social posts, best for first run
- bluesky: Bluesky Jetstream WebSocket source
- gdelt: GDELT news polling source
- all: run mock, Bluesky, and GDELT together

Set `PRODUCER_MODE` in `.env`.
