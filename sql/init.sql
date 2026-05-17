CREATE USER trends WITH PASSWORD 'trends';
CREATE DATABASE trends_db OWNER trends;

\connect trends_db;

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trend_windows (
    id BIGSERIAL,
    batch_id BIGINT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    keyword TEXT NOT NULL,
    source TEXT NOT NULL,
    mention_count BIGINT NOT NULL,
    avg_sentiment DOUBLE PRECISION NOT NULL,
    trend_score DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, window_start)
);

SELECT create_hypertable('trend_windows', 'window_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trend_windows_latest
ON trend_windows (window_end DESC, trend_score DESC);

CREATE INDEX IF NOT EXISTS idx_trend_windows_keyword_time
ON trend_windows (keyword, window_end DESC);

CREATE TABLE IF NOT EXISTS daily_trend_summary (
    summary_date DATE NOT NULL,
    keyword TEXT NOT NULL,
    source TEXT NOT NULL,
    total_mentions BIGINT NOT NULL,
    avg_sentiment DOUBLE PRECISION NOT NULL,
    max_trend_score DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (summary_date, keyword, source)
);

GRANT ALL PRIVILEGES ON DATABASE trends_db TO trends;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trends;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trends;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trends;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trends;
