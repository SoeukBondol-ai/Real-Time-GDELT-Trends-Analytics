from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ── DDL: ensure TimescaleDB hypertable exists ──────────────────────────
# These are DDL operations that the ORM can't handle (TimescaleDB-specific),
# so we keep them as raw SQL — but run via Airflow's SQLExecuteQueryOperator
# which uses the Airflow connection, not a hardcoded psycopg2.connect().
CREATE_HYPERTABLE_SQL = """
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.hypertables
    WHERE hypertable_name = 'trend_windows'
  ) THEN
    PERFORM create_hypertable('trend_windows', 'window_start');
  END IF;
END $$;
"""

# ── Analytics: daily summary aggregation ────────────────────────────────
# Complex window-function queries are more readable and maintainable in
# SQL than in SQLAlchemy ORM — so we use text() for these, but via
# SQLAlchemy sessions that give us connection pooling, Airflow connection
# management, and automatic session cleanup.
DAILY_SUMMARY_SQL = """
INSERT INTO daily_trend_summary (
    summary_date,
    keyword,
    source,
    total_mentions,
    avg_sentiment,
    max_trend_score,
    created_at
)
SELECT
    (CURRENT_DATE - INTERVAL '1 day')::DATE AS summary_date,
    keyword,
    source,
    SUM(mention_count)::BIGINT AS total_mentions,
    AVG(avg_sentiment)::DOUBLE PRECISION AS avg_sentiment,
    MAX(trend_score)::DOUBLE PRECISION AS max_trend_score,
    NOW() AS created_at
FROM (
    SELECT *
    FROM (
        SELECT
            tw.*,
            ROW_NUMBER() OVER (
                PARTITION BY window_start, window_end, keyword, source
                ORDER BY batch_id DESC, id DESC
            ) AS rn
        FROM trend_windows tw
        WHERE window_end >= CURRENT_DATE - INTERVAL '1 day'
          AND window_end < CURRENT_DATE
    ) x
    WHERE rn = 1
) deduped
GROUP BY keyword, source
ON CONFLICT (summary_date, keyword, source)
DO UPDATE SET
    total_mentions = EXCLUDED.total_mentions,
    avg_sentiment = EXCLUDED.avg_sentiment,
    max_trend_score = EXCLUDED.max_trend_score,
    created_at = NOW();
"""


def build_daily_summary() -> None:
    """Execute the daily summary rollup using SQLAlchemy via Airflow's PostgresHook."""
    from sqlalchemy import text

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="trends_db")
    with hook.get_sqlalchemy_engine().begin() as conn:
        conn.execute(text(DAILY_SUMMARY_SQL))


with DAG(
    dag_id="daily_trend_summary",
    description="Build daily summary rows from real-time trend windows",
    start_date=datetime(2024, 1, 1),
    schedule="15 0 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["trends", "summary"],
) as dag:
    ensure_hypertable = SQLExecuteQueryOperator(
        task_id="ensure_hypertable",
        conn_id="trends_db",
        sql=CREATE_HYPERTABLE_SQL,
    )

    build_summary = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )

    ensure_hypertable >> build_summary