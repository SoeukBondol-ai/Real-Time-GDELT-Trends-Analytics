# Real-Time Twitter Trends Analytics

A real-time data engineering pipeline for social trend analytics using Kafka, Spark Structured Streaming, TimescaleDB, Redis, MinIO, Airflow, FastAPI, and React.

Data flows from live public sources (Bluesky, GDELT, HackerNews) through Kafka into Spark for real-time keyword extraction and sentiment analysis, then into TimescaleDB and Redis for API serving via a React dashboard.

## Pipeline

```text
Bluesky / GDELT / HackerNews
        -> Kafka (raw-social-posts, raw-news-events)
        -> Spark Structured Streaming (tokenize, sentiment, windowed aggregation)
        -> TimescaleDB + MinIO + Kafka (trend-windows-1m)
        -> FastAPI + Redis
        -> React dashboard (live WebSocket updates)
```

## Technology stack

| Layer | Tool |
|---|---|
| Python package manager | uv (workspace) |
| Linting | Ruff |
| Frontend package manager | npm |
| Event streaming | Apache Kafka (KRaft mode) |
| Kafka inspection | Kafka UI |
| Stream processing | Apache Spark Structured Streaming |
| Orchestration | Apache Airflow |
| Analytics database | PostgreSQL with TimescaleDB |
| Cache | Redis |
| Object storage | MinIO |
| Backend API | FastAPI |
| Frontend | React, Vite, TypeScript |
| Database UI | pgAdmin |
| Devcontainer | VS Code Dev Containers |

## Folder structure

```text
.
├── .devcontainer/          # VS Code devcontainer config (LSP, Ruff, Python)
├── airflow/
│   ├── dags/               # Airflow DAGs
│   ├── plugins/
│   ├── Dockerfile
│   └── pyproject.toml
├── backend/
│   ├── app/                # FastAPI application
│   ├── Dockerfile
│   └── pyproject.toml
├── docs/
│   ├── architecture.md
│   └── slides.md
├── frontend/
│   ├── src/                # React + TypeScript dashboard
│   ├── Dockerfile
│   ├── nginx.conf
│   └── package.json
├── pgadmin/
│   └── servers.json
├── producer/
│   ├── producer/           # Kafka producers (Bluesky, GDELT, HackerNews)
│   ├── Dockerfile
│   └── pyproject.toml
├── spark/
│   └── jobs/
│       └── trend_streaming_job.py
├── sql/
│   ├── init.sql            # Database schema
│   └── test_query.sql      # Useful test queries
├── docker-compose.yml
├── pyproject.toml           # Root uv workspace config
├── Makefile
├── .env.example
└── README.md
```

## Requirements

- Docker Desktop or Docker Engine
- Docker Compose v2
- At least 8 GB RAM available for Docker
- Internet access for the first build (Docker images and Spark packages)
- [uv](https://docs.astral.sh/uv/) for local development and linting

## Quick start

### 1. Copy environment file

```bash
cp .env.example .env
```

Or:

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

The first run takes several minutes to download images and Spark connector packages.

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

## Devcontainer (VS Code LSP support)

Open this project in VS Code and use the Dev Containers extension to get full Python LSP (Pylance), Ruff linting, and autocomplete.

1. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Run `Dev Containers: Reopen in Container` from the Command Palette
3. The container runs `uv sync` automatically, installing all Python dependencies into a single `.venv`
4. Pylance will index `backend/app`, `producer/producer`, and `airflow/dags` for autocomplete

Start the devcontainer from the command line:

```bash
make devcontainer
```

## Data sources

All sources are free and require no API keys.

| Source | Type | Topic |
|---|---|---|
| [Bluesky Jetstream](https://docs.bsky.app/) | Real-time social posts | `raw-social-posts` |
| [GDELT](https://www.gdeltproject.org/) | Global news articles | `raw-news-events` |
| [HackerNews](https://hacker-news.firebaseio.com/) | Tech news stories | `raw-news-events` |

Configure in `.env`:

```bash
# Comma-separated: bluesky, hackernews, gdelt
PRODUCER_MODE=bluesky,hackernews,gdelt

GDELT_QUERY=(technology OR ai OR crypto OR climate OR election)
GDELT_POLL_SECONDS=60
HN_POLL_SECONDS=15
BLUESKY_JETSTREAM_URL=wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
```

Restart the producer after changing `.env`:

```bash
docker compose up -d --build producer
```

## Development

### Linting

Run Ruff checks and formatting:

```bash
make lint
```

Auto-fix issues:

```bash
make lint-fix
```

### Local Python environment

Install all dependencies (requires [uv](https://docs.astral.sh/uv/)):

```bash
uv sync
```

This creates a `.venv` with all workspace dependencies (backend, producer, airflow) plus dev tools (ruff, pytest).

## Kafka topics

| Topic | Purpose |
|---|---|
| raw-social-posts | Raw social messages from Bluesky |
| raw-news-events | Raw news from GDELT and HackerNews |
| raw-google-trends | Reserved for future Google Trends connector |
| processed-posts | Reserved for enriched post-level output |
| trend-windows-1m | One-minute processed trend windows |
| trend-windows-5m | Reserved for five-minute windows |
| sentiment-alerts | Reserved for sentiment alerts |
| pipeline-errors | Reserved for bad events and errors |

List topics:

```bash
make topics
```

## Database tables

Main database: `trends_db`

| Table | Purpose |
|---|---|
| trend_windows | Real-time trend output from Spark |
| daily_trend_summary | Daily summary generated by Airflow |

Connect from your host:

```bash
psql postgresql://trends:trends@localhost:5432/trends_db
```

Test query:

```sql
SELECT keyword, source, SUM(mention_count) AS mentions, SUM(trend_score) AS score
FROM trend_windows
WHERE window_end >= NOW() - INTERVAL '10 minutes'
GROUP BY keyword, source
ORDER BY score DESC
LIMIT 20;
```

## Airflow

Airflow contains one DAG: `daily_trend_summary`. It builds yesterday's summary from `trend_windows` into `daily_trend_summary`.

Airflow is not used for the streaming job. Spark streaming runs as its own long-running service.

## Spark streaming job

`spark/jobs/trend_streaming_job.py` does the following:

1. Reads from Kafka topics `raw-social-posts` and `raw-news-events`
2. Parses JSON events
3. Extracts keywords from text (filters out URLs, domains, stop words)
4. Calculates lexicon-based sentiment
5. Groups by 10-second event-time windows with 1-minute watermark
6. Writes trend windows to TimescaleDB and Kafka in update mode
7. Archives processed events to MinIO

To reset streaming state:

```bash
rm -rf spark/checkpoints
docker compose restart spark-trends
```

## Backend API

| Endpoint | Purpose |
|---|---|
| GET /health | Service health check |
| GET /api/trends/latest | Latest trend leaderboard |
| GET /api/trends/history/{keyword} | Trend history for a keyword |
| WS /ws/trends | Live dashboard updates |

## Control commands

| Command | Action |
|---|---|
| `make up` | Start the stack |
| `make down` | Stop the stack |
| `make restart` | Restart the stack |
| `make ps` | Show service status |
| `make logs` | Follow all logs |
| `make producer` | Follow producer logs |
| `make spark` | Follow Spark logs |
| `make backend` | Follow backend logs |
| `make lint` | Run ruff checks |
| `make lint-fix` | Run ruff and auto-fix |
| `make devcontainer` | Start devcontainer service |
| `make clean` | Stop and remove all volumes |

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

### Spark takes long on first start

Normal. Spark downloads Kafka, PostgreSQL, and S3 connector packages on first start.

### Kafka topics missing

```bash
docker compose up kafka-init
make topics
```

### Reset everything

```bash
make clean
cp .env.example .env
make up
```

## Notes for grading

This project demonstrates:

- event ingestion from free public APIs
- Kafka topics and partitions
- real-time stream processing with Spark Structured Streaming
- event-time window aggregation with watermarks
- stateful Spark streaming checkpoints
- time-series analytics storage (TimescaleDB)
- object storage archive (MinIO)
- API serving layer with caching (FastAPI + Redis)
- live WebSocket dashboard (React + Vite)
- orchestration with Airflow
- local development using Docker Compose and VS Code Dev Containers