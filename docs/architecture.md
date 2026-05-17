# Architecture

The project is designed as a local data engineering stack that looks like a production streaming platform.

```text
Social/news sources
  -> Producer service
  -> Kafka raw topics
  -> Spark Structured Streaming
  -> TimescaleDB for analytics
  -> MinIO for archived processed events
  -> Kafka processed topic
  -> FastAPI backend
  -> React dashboard
```

## Why each tool is used

| Tool | Purpose |
|---|---|
| Kafka | Durable event bus for raw and processed events |
| Spark Structured Streaming | Real-time window aggregation and trend scoring |
| TimescaleDB | Time-series storage for trend windows |
| Redis | Short-lived cache for live dashboard reads |
| MinIO | Local S3-compatible archive for processed events |
| Airflow | Scheduled daily summaries and operational workflows |
| FastAPI | REST and WebSocket API layer |
| React with Bun | Frontend dashboard |
| Kafka UI | Visual debugging for Kafka topics |
| pgAdmin | Visual database inspection |

## Data flow

1. The producer reads from mock data, Bluesky Jetstream, GDELT, or all sources.
2. Events are written to Kafka topics `raw-social-posts` and `raw-news-events`.
3. Spark reads the raw topics, extracts keywords, scores sentiment, and groups events in one-minute windows.
4. Spark writes results to TimescaleDB and the Kafka topic `trend-windows-1m`.
5. Spark also archives processed events to MinIO.
6. FastAPI reads latest trend windows from TimescaleDB and caches the response in Redis.
7. React receives live updates through WebSocket.
8. Airflow can build daily summaries from the streaming table.
