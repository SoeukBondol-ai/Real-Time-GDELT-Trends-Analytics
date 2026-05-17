# Real-Time Twitter Trends Analytics

A complete local data engineering final project for real-time social trend analytics.

This project uses Kafka, Spark Structured Streaming, TimescaleDB, Redis, MinIO, Airflow, FastAPI, and a React dashboard built with Bun. It is designed to run with Docker Compose and to be easy to explain in a final project presentation.

The system can run fully offline with a mock producer. It can also use free public data sources such as Bluesky Jetstream and GDELT when internet access is available.

## What this project does

The project simulates and processes a live social media trend pipeline:

```text
Mock / Bluesky / GDELT data
        -> Kafka
        -> Spark Structured Streaming
        -> TimescaleDB + MinIO + Kafka processed topic
        -> FastAPI + Redis
        -> React dashboard
```

The dashboard shows:

- live top keywords
- source of each trend
- mention counts
- simple sentiment labels
- trend scores
- latest stream status

## Technology stack

| Layer | Tool |
|---|---|
| Python package manager | uv |
| Frontend package manager | Bun |
| Event streaming | Apache Kafka in KRaft mode |
| Kafka inspection | Kafka UI |
| Stream processing | Apache Spark Structured Streaming |
| Orchestration | Apache Airflow |
| Analytics database | PostgreSQL with TimescaleDB |
| Cache | Redis |
| Object storage | MinIO |
| Backend API | FastAPI |
| Frontend | React, Vite, Bun |
| Database UI | pgAdmin |

## Folder structure

```text
.
├── airflow/
│   ├── dags/
│   ├── plugins/
│   ├── Dockerfile
│   └── requirements.txt
├── backend/
│   ├── app/
│   ├── Dockerfile
│   └── pyproject.toml
├── docs/
│   └── architecture.md
├── frontend/
│   ├── src/
│   ├── Dockerfile
│   ├── package.json
│   └── nginx.conf
├── pgadmin/
│   └── servers.json
├── producer/
│   ├── producer/
│   ├── Dockerfile
│   └── pyproject.toml
├── spark/
│   └── jobs/
│       └── trend_streaming_job.py
├── sql/
│   └── init.sql
├── docker-compose.yml
├── Makefile
├── .env.example
└── README.md
```

## Requirements

Install these before running the project:

- Docker Desktop or Docker Engine
- Docker Compose v2
- At least 8 GB RAM available for Docker
- Internet access for the first build because Docker images and Spark packages must be downloaded

Recommended Docker memory setting: 8 GB or more.

## Quick start

### 1. Copy environment file

```bash
cp .env.example .env
```

Or use Make:

```bash
make copy-env
```

### 2. Start the stack

```bash
docker compose up -d --build
```

Or:

```bash
make up
```

### 3. Wait for services to start

The first run can take several minutes because images and Spark connector packages are downloaded.

Check status:

```bash
docker compose ps
```

Follow logs:

```bash
docker compose logs -f
```

### 4. Open the dashboard

| Service | URL |
|---|---|
| React dashboard | http://localhost:5173 |
| FastAPI docs | http://localhost:8000/docs |
| Kafka UI | http://localhost:8082 |
| Airflow | http://localhost:8080 |
| Spark master UI | http://localhost:8090 |
| pgAdmin | http://localhost:8081 |
| MinIO console | http://localhost:9001 |

Default credentials:

| Service | Username | Password |
|---|---|---|
| Airflow | admin | admin |
| pgAdmin | admin@admin.com | admin |
| MinIO | minio | minio12345 |

## First run mode

The default producer mode is `mock`.

This means the project will generate local fake social posts and send them into Kafka. This is the safest mode for demo and grading because it does not depend on an external API.

In `.env`:

```bash
PRODUCER_MODE=mock
```

## Use real public sources

### Bluesky Jetstream

Bluesky Jetstream is a free Twitter-like public social stream.

Change `.env`:

```bash
PRODUCER_MODE=bluesky
```

Then restart only the producer:

```bash
docker compose up -d --build producer
```

### GDELT news source

GDELT is a free global news data source.

Change `.env`:

```bash
PRODUCER_MODE=gdelt
GDELT_QUERY=technology OR ai OR climate OR crypto
```

Restart:

```bash
docker compose up -d --build producer
```

### HackerNews source

HackerNews is a free tech news source.

Change `.env`:

```bash
PRODUCER_MODE=hackernews
```

Restart:

```bash
docker compose up -d --build producer
```

### Note on API Keys
Both **Bluesky Jetstream** and **HackerNews** APIs used in this project are **100% Free** and **do not require API keys**. You do not need to sign up for anything to use them.

If you decide to add other sources (like NewsAPI.org) in the future, you should add your keys to the `.env` file and read them in `producer/producer/config.py`.

### Run all sources

```bash
PRODUCER_MODE=all
```

This runs mock data, Bluesky, GDELT, and HackerNews together.

## Important Kafka topics

| Topic | Purpose |
|---|---|
| raw-social-posts | Raw social messages from mock or Bluesky |
| raw-news-events | Raw news events from GDELT |
| raw-google-trends | Reserved for a Google Trends connector |
| processed-posts | Reserved for enriched post-level output |
| trend-windows-1m | One-minute processed trend windows |
| trend-windows-5m | Reserved for five-minute windows |
| sentiment-alerts | Reserved for sentiment alerts |
| pipeline-errors | Reserved for bad events and errors |

List topics:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Or:

```bash
make topics
```

## Database tables

The main database is `trends_db`.

Main tables:

| Table | Purpose |
|---|---|
| trend_windows | Real-time trend output from Spark |
| daily_trend_summary | Daily summary generated by Airflow |

Connect from your host:

```bash
psql postgresql://trends:trends@localhost:5432/trends_db
```

Useful query:

```sql
SELECT keyword, source, SUM(mention_count) AS mentions, SUM(trend_score) AS score
FROM trend_windows
WHERE window_end >= NOW() - INTERVAL '10 minutes'
GROUP BY keyword, source
ORDER BY score DESC
LIMIT 20;
```

## Airflow

Airflow is included for orchestration. It contains one DAG:

```text
daily_trend_summary
```

This DAG builds yesterday's summary from `trend_windows` into `daily_trend_summary`.

Airflow is not used to run the never-ending streaming job. That is intentional. Airflow is best for scheduled jobs, backfills, summaries, and checks. Spark streaming is run as its own long-running service.

## Spark streaming job

The Spark job is here:

```text
spark/jobs/trend_streaming_job.py
```

It does the following:

1. Reads from Kafka topics `raw-social-posts` and `raw-news-events`.
2. Parses JSON events.
3. Extracts keywords from text.
4. Calculates simple lexicon sentiment.
5. Groups by one-minute event-time windows.
6. Writes trend windows to TimescaleDB in update mode for fast demo visibility.
7. Publishes processed trend windows to Kafka.
8. Archives processed events to MinIO.

Spark checkpoints are stored in:

```text
spark/checkpoints/
```

The database queries deduplicate Spark update-mode rows by keeping the latest batch per keyword, source, and window.

If you want to reset streaming state during development:

```bash
rm -rf spark/checkpoints
```

Then restart Spark:

```bash
docker compose restart spark-trends
```

## Backend API

FastAPI endpoints:

| Endpoint | Purpose |
|---|---|
| GET /health | Service health check |
| GET /api/trends/latest | Latest trend leaderboard |
| GET /api/trends/history/{keyword} | Trend history for one keyword |
| WS /ws/trends | Live dashboard updates |

Open API documentation:

```text
http://localhost:8000/docs
```

## Frontend

The frontend is a React dashboard built with Bun and Vite.

Local service URL:

```text
http://localhost:5173
```

The dashboard uses WebSocket updates from the backend.

## Control commands

Start everything:

```bash
make up
```

Stop everything:

```bash
make down
```

Restart everything:

```bash
make restart
```

Show service status:

```bash
make ps
```

Follow all logs:

```bash
make logs
```

Follow only producer logs:

```bash
make producer
```

Follow only Spark logs:

```bash
make spark
```

Follow only backend logs:

```bash
make backend
```

Remove containers and volumes:

```bash
make clean
```

## Recommended demo flow

Use this order during your final presentation:

1. Show the architecture diagram in `docs/architecture.md`.
2. Open Kafka UI and show the raw topics.
3. Open producer logs and show messages being produced.
4. Open Spark logs and explain keyword extraction, sentiment, and windows.
5. Open pgAdmin and show the `trend_windows` table.
6. Open the React dashboard and show live trend changes.
7. Open Airflow and explain the daily summary DAG.
8. Open MinIO and show archived processed events.

## Troubleshooting

### Dashboard shows no data

Check the producer:

```bash
docker compose logs -f producer
```

Check Spark:

```bash
docker compose logs -f spark-trends
```

Check the database:

```bash
docker compose exec postgres psql -U trends -d trends_db -c "SELECT COUNT(*) FROM trend_windows;"
```

### Spark takes a long time on first start

This is normal. Spark downloads Kafka, PostgreSQL, and S3 connector packages on first start.

### Kafka topics are missing

Run:

```bash
docker compose up kafka-init
```

Then check:

```bash
make topics
```

### Port already in use

Change the port mapping in `docker-compose.yml` or stop the service using that port.

Common ports used by this project:

```text
5173, 8000, 8080, 8081, 8082, 8090, 9000, 9001, 5432, 6379, 19092
```

### Reset everything

```bash
docker compose down -v
rm -rf logs data spark/checkpoints spark/warehouse
cp .env.example .env
make up
```

## Notes for grading

This project demonstrates these data engineering concepts:

- event ingestion
- Kafka topics and partitions
- real-time stream processing
- event-time window aggregation
- stateful Spark streaming checkpoints
- time-series analytics storage
- object storage archive
- API serving layer
- live WebSocket dashboard
- orchestration with Airflow
- local development using Docker Compose

## Possible future improvements

- Add a real Twitter/X connector if API access is available.
- Add a Google Trends connector.
- Add language detection.
- Add named entity recognition.
- Add stronger sentiment analysis with a machine learning model.
- Add Grafana dashboards.
- Add data quality checks with Great Expectations.
- Add dead-letter handling to `pipeline-errors`.
