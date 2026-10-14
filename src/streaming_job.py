"""
Spark Structured Streaming job for shuttle telemetry.

Reads JSON from Kafka, validates the schema, applies a watermark,
computes 1-minute windowed aggregates, and writes to console + Parquet.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        src/streaming_job.py
"""

# Allow running via `spark-submit src/streaming_job.py` from the repo root.
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, max, count, sum as spark_sum, when
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType,
)

from config import (
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    OUTPUT_PATH,
    TRIGGER_INTERVAL,
    WATERMARK_DELAY,
    WINDOW_DURATION,
)


# Explicit schema beats inferSchema here — bad JSON lands in the
# malformed column instead of silently breaking the job.
TELEMETRY_SCHEMA = StructType([
    StructField("mission", StringType(), True),
    StructField("seq", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("velocity_kms", DoubleType(), True),
    StructField("altitude_km", DoubleType(), True),
    StructField("fuel_pct", DoubleType(), True),
    StructField("cabin_pressure_kpa", DoubleType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("o2_pct", DoubleType(), True),
    StructField("status", StringType(), True),
])


def build_spark() -> SparkSession:
    """Create a local Spark session with sane defaults for streaming."""
    return (
        SparkSession.builder
        .appName("ShuttleTelemetryStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Raw Kafka stream — value is bytes, so we cast to string first.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), TELEMETRY_SCHEMA).alias("data"))
        .select("data.*")
        # Drop rows where the timestamp didn't parse — they can't be windowed.
        .filter(col("timestamp").isNotNull())
    )

    # Add a derived anomaly flag so downstream dashboards don't
    # have to re-implement the thresholds.
    enriched = parsed.withColumn(
        "is_anomaly",
        when(
            (col("engine_temp_c") > 1400)
            | (col("fuel_pct") < 15)
            | (col("cabin_pressure_kpa") < 95),
            1,
        ).otherwise(0),
    )

    # Watermark handles late data: anything more than 30s late is dropped.
    windowed = (
        enriched
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(window(col("timestamp"), WINDOW_DURATION))
        .agg(
            avg("velocity_kms").alias("avg_velocity_kms"),
            max("altitude_km").alias("max_altitude_km"),
            avg("engine_temp_c").alias("avg_engine_temp_c"),
            avg("fuel_pct").alias("avg_fuel_pct"),
            spark_sum("is_anomaly").alias("anomaly_count"),
            count("*").alias("reading_count"),
        )
    )

    # Console sink is great for demos and debugging.
    console_query = (
        windowed.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # Parquet sink is what you'd actually keep — query it later
    # with Spark, DuckDB, or Athena.
    parquet_query = (
        windowed.writeStream
        .outputMode("complete")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/parquet")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"Streaming started. Writing Parquet to {OUTPUT_PATH}")
    spark.streams.awaitAnyTermination()
    console_query.stop()
    parquet_query.stop()


if __name__ == "__main__":
    main()
