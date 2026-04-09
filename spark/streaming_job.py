"""
streaming_job.py — Spark Structured Streaming for the IoT pipeline.

Two queries run in parallel:
  raw_query  — Kafka → quality checks → sensor_readings   (30s trigger)
  agg_query  — Kafka → 5-min windows  → sensor_aggregates (5min trigger)

Checkpoints at /tmp/spark-checkpoints/{raw,aggregates} allow restart
from the last committed offset without reprocessing.
"""

import logging
import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("streaming_job")


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        log.error("Required environment variable %s is not set", name)
        sys.exit(1)
    return val


KAFKA_BROKER   = os.environ.get("KAFKA_BROKER",   "kafka:9092")
KAFKA_TOPIC    = os.environ.get("KAFKA_TOPIC",    "sensor-data")
POSTGRES_USER  = _require_env("POSTGRES_USER")
POSTGRES_PASS  = _require_env("POSTGRES_PASSWORD")
POSTGRES_DB    = os.environ.get("POSTGRES_DB",    "iot_sensors")
POSTGRES_HOST  = os.environ.get("POSTGRES_HOST",  "timescaledb")
POSTGRES_PORT  = os.environ.get("POSTGRES_PORT",  "5432")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    "?reWriteBatchedInserts=true"
)

JDBC_PROPS = {
    "user":       POSTGRES_USER,
    "password":   POSTGRES_PASS,
    "driver":     "org.postgresql.Driver",
    "stringtype": "unspecified",
}

CHECKPOINT_BASE  = "/tmp/spark-checkpoints"
WATERMARK_DELAY  = "10 minutes"
WINDOW_DURATION  = "5 minutes"
RAW_TRIGGER_SECS = 30
AGG_TRIGGER_MINS = 5

DQ_TEMP_MIN:  float = -40.0
DQ_TEMP_MAX:  float =  100.0
DQ_HUM_MIN:   float =    0.0
DQ_HUM_MAX:   float =  100.0
DQ_PRES_MIN:  float =  870.0
DQ_PRES_MAX:  float = 1085.0

MESSAGE_SCHEMA = StructType([
    StructField("sensor_id",   StringType(),  nullable=True),
    StructField("timestamp",   StringType(),  nullable=True),
    StructField("temperature", DoubleType(),  nullable=True),
    StructField("humidity",    DoubleType(),  nullable=True),
    StructField("pressure",    DoubleType(),  nullable=True),
    StructField("location",    StringType(),  nullable=True),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("iot-streaming-job")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.ui.enabled", "true")
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )


def parse_messages(kafka_df: DataFrame) -> DataFrame:
    return (
        kafka_df
        .select(
            F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("d"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
        )
        .select("d.*", "topic", "partition", "offset")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
    )


def apply_quality_checks(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("sensor_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("temperature").isNotNull())
        .filter(F.col("humidity").isNotNull())
        .filter(F.col("pressure").isNotNull())
        .filter(F.length(F.col("sensor_id")) > 0)
        .filter(F.col("temperature").between(DQ_TEMP_MIN, DQ_TEMP_MAX))
        .filter(F.col("humidity").between(DQ_HUM_MIN,  DQ_HUM_MAX))
        .filter(F.col("pressure").between(DQ_PRES_MIN, DQ_PRES_MAX))
    )


def _make_write_raw_fn(jdbc_url: str, jdbc_props: dict):
    def write_raw(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            log.debug("raw batch %d is empty — skipping", batch_id)
            return

        output_df = batch_df.select(
            F.col("sensor_id"),
            F.col("event_time").alias("timestamp"),
            F.col("temperature"),
            F.col("humidity"),
            F.col("pressure"),
        )

        log.info("raw batch %d — writing %d rows to sensor_readings", batch_id, output_df.count())
        output_df.write.jdbc(url=jdbc_url, table="sensor_readings", mode="append", properties=jdbc_props)
        log.info("raw batch %d — committed", batch_id)

    return write_raw


def _make_write_agg_fn(jdbc_url: str, jdbc_props: dict):
    def write_agg(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            log.debug("agg batch %d is empty — skipping", batch_id)
            return

        log.info("agg batch %d — writing %d window rows to sensor_aggregates", batch_id, batch_df.count())
        batch_df.write.jdbc(url=jdbc_url, table="sensor_aggregates", mode="append", properties=jdbc_props)
        log.info("agg batch %d — committed", batch_id)

    return write_agg


def build_aggregations(clean_df: DataFrame) -> DataFrame:
    """
    5-minute tumbling window aggregations with a 10-minute watermark.
    append output mode ensures each (sensor_id, window_start) is emitted
    once after the watermark passes the window end.
    """
    windowed = (
        clean_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION),
            F.col("sensor_id"),
        )
        .agg(
            F.round(F.avg("temperature"),  2).alias("avg_temp"),
            F.round(F.min("temperature"),  2).alias("min_temp"),
            F.round(F.max("temperature"),  2).alias("max_temp"),
            F.round(F.coalesce(F.stddev("temperature"), F.lit(0.0)), 2).alias("stddev_temp"),
            F.round(F.avg("humidity"),  2).alias("avg_humidity"),
            F.round(F.min("humidity"),  2).alias("min_humidity"),
            F.round(F.max("humidity"),  2).alias("max_humidity"),
            F.count("*").cast("integer").alias("reading_count"),
        )
    )

    return windowed.select(
        F.col("sensor_id"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("avg_temp"),
        F.col("min_temp"),
        F.col("max_temp"),
        F.col("stddev_temp"),
        F.col("avg_humidity"),
        F.col("min_humidity"),
        F.col("max_humidity"),
        F.col("reading_count"),
    )


def main() -> None:
    log.info("Starting IoT Structured Streaming job")
    log.info("Kafka: %s  topic: %s", KAFKA_BROKER, KAFKA_TOPIC)
    log.info("TimescaleDB: %s/%s", POSTGRES_HOST, POSTGRES_DB)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Query 1: raw readings
    raw_kafka_df  = read_kafka_stream(spark)
    raw_clean_df  = apply_quality_checks(parse_messages(raw_kafka_df))

    raw_query = (
        raw_clean_df.writeStream
        .outputMode("append")
        .foreachBatch(_make_write_raw_fn(JDBC_URL, JDBC_PROPS))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
        .trigger(processingTime=f"{RAW_TRIGGER_SECS} seconds")
        .queryName("raw-readings-to-timescaledb")
        .start()
    )
    log.info("raw_query started — id=%s", raw_query.id)

    # Query 2: windowed aggregations
    agg_kafka_df = read_kafka_stream(spark)
    agg_df       = build_aggregations(apply_quality_checks(parse_messages(agg_kafka_df)))

    agg_query = (
        agg_df.writeStream
        .outputMode("append")
        .foreachBatch(_make_write_agg_fn(JDBC_URL, JDBC_PROPS))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/aggregates")
        .trigger(processingTime=f"{AGG_TRIGGER_MINS} minutes")
        .queryName("window-aggregates-to-timescaledb")
        .start()
    )
    log.info("agg_query started  — id=%s", agg_query.id)

    try:
        spark.streams.awaitAnyTermination()
    except Exception as exc:  # noqa: BLE001
        log.error("A streaming query terminated with an error: %s", exc, exc_info=True)
        raise
    finally:
        for q in spark.streams.active:
            log.info("Query %s last progress: %s", q.name, q.lastProgress)
        spark.stop()
        log.info("SparkSession stopped")


if __name__ == "__main__":
    main()
