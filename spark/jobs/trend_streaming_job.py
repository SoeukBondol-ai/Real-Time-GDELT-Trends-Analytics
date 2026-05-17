import os
import re
from typing import Iterable, Optional, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    array,
    avg,
    coalesce,
    col,
    current_timestamp,
    explode,
    from_json,
    lit,
    regexp_replace,
    struct,
    sum as spark_sum,
    to_json,
    to_timestamp,
    udf,
    window,
)
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

STOP_WORDS = {
    "a", "about", "after", "all", "also", "an", "and", "are", "as", "at", "be", "because",
    "but", "by", "can", "for", "from", "has", "have", "i", "if", "in", "is", "it", "its",
    "new", "no", "not", "of", "on", "or", "our", "that", "the", "their", "this", "to", "today",
    "was", "we", "with", "you", "your", "will", "http", "https", "rt", "get", "got", "just",
    "like", "make", "know", "think", "see", "one", "good", "great", "well", "more", "much",
    "even", "now", "still", "time", "day", "way", "people", "would", "could", "should",
    "want", "need", "take", "give", "back", "come", "done", "real", "post", "social",
    "media", "really", "actually", "going", "been", "being", "they", "them", "these", "those",
}

POSITIVE = {
    "good", "great", "growth", "gain", "gains", "improve", "improved", "strong", "win", "wins",
    "launch", "fast", "success", "positive", "best", "safe", "secure", "stable",
}
NEGATIVE = {
    "bad", "bug", "bugs", "crash", "down", "fail", "failed", "issue", "issues", "loss", "risk",
    "slow", "negative", "worse", "worst", "attack", "outage", "error",
}


def tokenize(text: Optional[str]) -> List[str]:
    if not text:
        return []
    cleaned = re.sub(r"https?://\S+", " ", text.lower())
    tokens = re.findall(r"#?[a-z][a-z0-9_]{2,}", cleaned)
    result: list[str] = []
    for token in tokens:
        token = token.lstrip("#")
        if token not in STOP_WORDS and len(token) >= 3:
            result.append(token)
    return result[:20]


def sentiment(text: Optional[str]) -> float:
    tokens = tokenize(text)
    if not tokens:
        return 0.0
    pos = sum(1 for token in tokens if token in POSITIVE)
    neg = sum(1 for token in tokens if token in NEGATIVE)
    return float((pos - neg) / max(len(tokens), 1))


keywords_udf = udf(tokenize, ArrayType(StringType()))
sentiment_udf = udf(sentiment, DoubleType())


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def foreach_batch_writer(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    jdbc_url = env("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/trends_db")
    postgres_user = env("POSTGRES_USER", "trends")
    postgres_password = env("POSTGRES_PASSWORD", "trends")
    kafka_bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    flattened = (
        batch_df.select(
            lit(batch_id).alias("batch_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("keyword"),
            col("source"),
            col("mention_count"),
            col("avg_sentiment"),
            col("trend_score"),
            current_timestamp().alias("created_at"),
        )
        .filter(col("keyword").isNotNull())
    )

    flattened.write.format("jdbc").option("url", jdbc_url).option("dbtable", "trend_windows").option(
        "user", postgres_user
    ).option("password", postgres_password).option("driver", "org.postgresql.Driver").mode("append").save()

    kafka_payload = flattened.select(
        col("keyword").cast("string").alias("key"),
        to_json(
            struct(
                "batch_id",
                "window_start",
                "window_end",
                "keyword",
                "source",
                "mention_count",
                "avg_sentiment",
                "trend_score",
                "created_at",
            )
        ).alias("value"),
    )

    kafka_payload.write.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap).option(
        "topic", "trend-windows-1m"
    ).save()


def build_spark() -> SparkSession:
    minio_endpoint = env("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = env("MINIO_ACCESS_KEY", "minio")
    minio_secret_key = env("MINIO_SECRET_KEY", "minio12345")

    return (
        SparkSession.builder.appName("real-time-twitter-trends")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    minio_bucket = env("MINIO_BUCKET", "trends-raw")

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("source", StringType(), True),
            StructField("author", StringType(), True),
            StructField("text", StringType(), True),
            StructField("lang", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "raw-social-posts,raw-news-events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("event"))
        .select("event.*")
        .withColumn("created_at_clean", regexp_replace(col("created_at"), "Z$", ""))
        .withColumn("event_time", coalesce(to_timestamp(col("created_at_clean")), current_timestamp()))
        .withColumn("source", coalesce(col("source"), lit("unknown")))
        .withColumn("sentiment", sentiment_udf(col("text")))
        .withColumn("keywords", keywords_udf(col("text")))
    )

    exploded = parsed.select(
        "id",
        "source",
        "author",
        "text",
        "lang",
        "created_at",
        "url",
        "event_time",
        "sentiment",
        explode(col("keywords")).alias("keyword"),
    )

    trends = (
        exploded.withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("keyword"), col("source"))
        .agg(
            spark_sum(lit(1)).alias("mention_count"),
            avg(col("sentiment")).alias("avg_sentiment"),
        )
        .withColumn("trend_score", col("mention_count") * (lit(1.0) + spark_abs(col("avg_sentiment"))))
    )

    checkpoint_dir = "s3a://trends-raw/checkpoints/trends_v2"
    trend_query = (
        trends.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_dir)
        .foreachBatch(foreach_batch_writer)
        .start()
    )

    archive_query = (
        parsed.select("id", "source", "author", "text", "lang", "created_at", "url", "event_time", "sentiment")
        .writeStream.outputMode("append")
        .format("json")
        .option("path", f"s3a://{minio_bucket}/processed-events")
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/minio-archive")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
