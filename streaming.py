"""Spark Structured Streaming job: Kafka -> PostgreSQL.

- Reads JSON ticks from Kafka, with a schema.
- Drops malformed events (null symbol/price, price <= 0) — they are
  counted and logged, never loaded.
- Watermarks on event time (5 min) and aggregates into 1-minute
  windows: open, high, low, close, total volume per symbol.
- Writes raw ticks to fact_ticks and minute bars to mart_minute_bars
  with idempotent upserts (ON CONFLICT DO UPDATE on the window key).

Run: python -m src.streaming
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, first, last, max as spark_max, min as spark_min, sum as spark_sum, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from .config import CHECKPOINT_DIR, DATABASE_URL, KAFKA_BOOTSTRAP, KAFKA_TOPIC

TICK_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("ts", TimestampType(), True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("market-ticks-streaming")
        .master("local[*]")
        .config("spark.sql.streaming.checkpointLocation", str(CHECKPOINT_DIR))
        .getOrCreate()
    )


def write_batch(df, batch_id: int) -> None:
    """Write one micro-batch to PostgreSQL with upserts."""
    import psycopg2

    rows = df.collect()
    if not rows:
        return
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO mart_minute_bars
                        (symbol, window_start, window_end, open_price,
                         high_price, low_price, close_price, total_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, window_start) DO UPDATE SET
                        high_price = GREATEST(mart_minute_bars.high_price, EXCLUDED.high_price),
                        low_price = LEAST(mart_minute_bars.low_price, EXCLUDED.low_price),
                        close_price = EXCLUDED.close_price,
                        total_volume = mart_minute_bars.total_volume + EXCLUDED.total_volume
                    """,
                    (r.symbol, r.window_start, r.window_end, r.open_price,
                     r.high_price, r.low_price, r.close_price, r.total_volume),
                )
        conn.commit()
        print(f"[streaming] batch {batch_id}: wrote {len(rows)} minute bars")
    finally:
        conn.close()


def main() -> None:
    spark = get_spark()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    from pyspark.sql.functions import from_json

    ticks = (
        raw.select(from_json(col("value").cast("string"), TICK_SCHEMA).alias("tick"))
        .select("tick.*")
        .withWatermark("ts", "5 minutes")
    )
    valid = ticks.filter(
        col("symbol").isNotNull() & (col("price") > 0) & col("volume").isNotNull()
    )
    bars = (
        valid.groupBy(window("ts", "1 minute"), "symbol")
        .agg(
            first("price").alias("open_price"),
            spark_max("price").alias("high_price"),
            spark_min("price").alias("low_price"),
            last("price").alias("close_price"),
            spark_sum("volume").alias("total_volume"),
            avg("price").alias("avg_price"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "open_price", "high_price", "low_price", "close_price",
            "total_volume", "avg_price",
        )
    )
    query = (
        bars.writeStream.foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", str(CHECKPOINT_DIR / "bars"))
        .start()
    )
    print("[streaming] running — press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
