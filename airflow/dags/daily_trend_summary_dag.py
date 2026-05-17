from __future__ import annotations

import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def build_daily_summary() -> None:
    conn = psycopg2.connect(
        host=os.getenv("TRENDS_DB_HOST", "postgres"),
        port=int(os.getenv("TRENDS_DB_PORT", "5432")),
        dbname=os.getenv("TRENDS_DB_NAME", "trends_db"),
        user=os.getenv("TRENDS_DB_USER", "trends"),
        password=os.getenv("TRENDS_DB_PASSWORD", "trends"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                )
    finally:
        conn.close()


with DAG(
    dag_id="daily_trend_summary",
    description="Build daily summary rows from real-time trend windows",
    start_date=datetime(2024, 1, 1),
    schedule_interval="15 0 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["trends", "summary"],
) as dag:
    PythonOperator(task_id="build_daily_summary", python_callable=build_daily_summary)
