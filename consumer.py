"""Spark consumer with a transactional, five-minute event-time watermark."""

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
)
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, LongType, StructField, StructType, TimestampType

from database import commit_stream_batch, stream_event_time_state
from event_watermark import GRACE_SECONDS, is_beyond_grace, window_bounds
from observability import record_progress, stage_event
from quality import alert_if_needed, check_ratings_spark

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT = os.getenv("CHECKPOINT_ROOT", "checkpoints/stream_grace5")
RESULTS = os.getenv("DATABASE_PATH", "data/analytics.sqlite3") + ":hourly_counts"
WATERMARK_MINUTES = GRACE_SECONDS // 60

schema = StructType([
    StructField("userId", LongType(), False),
    StructField("movieId", LongType(), False),
    StructField("rating", DoubleType(), False),
    StructField("timestamp", LongType(), False),
])

spark = (
    SparkSession.builder.appName("MovieLensStreamDeepDive")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8")
    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


class ProgressLog(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        os.makedirs("reports", exist_ok=True)
        with open("reports/stream_progress.jsonl", "a", encoding="utf-8") as handle:
            handle.write(event.progress.json + "\n")
        try:
            record_progress(json.loads(event.progress.json))
        except Exception as exc:
            print(f"[ALERT] progress database write failed: {exc}", flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        if event.exception:
            print(f"[ALERT] streaming query failed: {event.exception}", flush=True)


spark.streams.addListener(ProgressLog())


def ensure_ratings_topic():
    for attempt in range(30):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA, client_id="ratings-init")
            try:
                admin.create_topics([NewTopic("ratings", num_partitions=1, replication_factor=1)])
                print("Created Kafka topic 'ratings'", flush=True)
            except TopicAlreadyExistsError:
                print("Kafka topic 'ratings' already exists", flush=True)
            admin.close()
            return
        except Exception as exc:
            print(f"Waiting for Kafka ({attempt + 1}/30): {exc}", flush=True)
            time.sleep(2)
    raise RuntimeError("Could not create or verify Kafka topic 'ratings'")


ensure_ratings_topic()


def parse_events():
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "ratings")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "100000"))
        .option("failOnDataLoss", "true")
        .load()
    )
    return (
        raw.select(
            from_json(col("value").cast("string"), schema).alias("r"),
            col("offset").alias("_kafka_offset"),
            col("partition").alias("_kafka_partition"),
            col("value").cast("string").alias("_raw_json"),
        )
        .select("r.*", "_kafka_offset", "_kafka_partition", "_raw_json")
        .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))
    )


def _utc_sql_time(epoch_seconds):
    return datetime.fromtimestamp(int(epoch_seconds), timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def publish_watermark():
    maximum = stream_event_time_state()
    if maximum is None:
        return
    os.makedirs("reports", exist_ok=True)
    path = "reports/event_watermark.json"
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump({"max_event_time": maximum, "watermark_epoch": maximum - GRACE_SECONDS,
                   "updated_at_epoch": time.time()}, handle)
    os.replace(temporary, path)


def process_microbatch(batch_df, batch_id):
    """Validate and classify Kafka rows against the previously observed watermark."""
    batch_df.persist()
    try:
        rows_in = batch_df.count()
        checks = {"missing": 0, "range": 0, "ids": 0, "duplicates": 0}
        if rows_in:
            good, bad, checks = check_ratings_spark(batch_df.drop("event_time"))
            quality_failed = bad.count()
            valid_rows = good.count()
        else:
            good = bad = batch_df
            quality_failed = valid_rows = 0

        alert_if_needed(quality_failed, rows_in, f"consumer-batch-{batch_id}")
        if quality_failed:
            bad.write.mode("append").parquet("bad_ratings_stream")

        previous_max = stream_event_time_state()
        max_event_time = None
        within_grace = 0
        late_records = []
        hourly_rows = []

        if valid_rows:
            # This topic has one partition; source offset is its arrival order.
            # Streaming rows are processed iteratively so the tested pure-Python
            # window-boundary rule and the persisted watermark are identical.
            high_watermark = previous_max
            totals = defaultdict(int)
            ordered = good.orderBy(col("_kafka_partition"), col("_kafka_offset")) \
                .select("userId", "movieId", "rating", "timestamp", "_kafka_partition",
                        "_kafka_offset", "_raw_json").toLocalIterator()
            for row in ordered:
                event_timestamp = int(row.timestamp)
                window_start, window_end = window_bounds(event_timestamp)
                if is_beyond_grace(event_timestamp, high_watermark):
                    late_records.append({
                        "userId": int(row.userId), "movieId": int(row.movieId),
                        "rating": float(row.rating), "timestamp": event_timestamp,
                        "_kafka_partition": int(row._kafka_partition),
                        "_kafka_offset": int(row._kafka_offset),
                        "_window_start": window_start, "_window_end": window_end,
                        "_watermark_at_arrival": int(high_watermark) - GRACE_SECONDS,
                        "_late_seconds": int(high_watermark) - window_end - GRACE_SECONDS,
                        "_row_json": row._raw_json,
                    })
                else:
                    within_grace += 1
                    totals[(window_start, window_end, int(row.movieId))] += 1
                high_watermark = event_timestamp if high_watermark is None else max(high_watermark, event_timestamp)

            max_event_time = high_watermark
            hourly_rows = [
                (_utc_sql_time(start), _utc_sql_time(end), movie_id, n)
                for (start, end, movie_id), n in totals.items()
            ]

        bad_samples = [row.asDict(recursive=True) for row in bad.limit(20).collect()] if quality_failed else []
        result = commit_stream_batch(
            batch_id=batch_id, rows_in=rows_in, quality_failed=quality_failed,
            checks=checks, within_rows=within_grace, late_rows=late_records,
            hourly_counts=hourly_rows, max_event_time=max_event_time, samples=bad_samples,
        )
        if result.get("replayed"):
            publish_watermark()
            print(f"[WATERMARK] batch={batch_id} already committed; replay skipped", flush=True)
            return
        publish_watermark()

        conservation = rows_in == within_grace + len(late_records) + quality_failed
        watermark = result.get("watermark")
        watermark_text = "none" if watermark is None else datetime.fromtimestamp(watermark, timezone.utc).isoformat()
        print(
            f"[WATERMARK] batch={batch_id} incoming={rows_in} valid={valid_rows} "
            f"within_5m={within_grace} quarantined_late={len(late_records)} "
            f"quality_failed={quality_failed} conservation={'PASS' if conservation else 'FAIL'} "
            f"watermark={watermark_text}", flush=True,
        )
        if not conservation:
            raise RuntimeError("stream row conservation check failed after commit")
        stage_event("stream-watermark", "batch_committed", {
            "batch_id": batch_id, "incoming": rows_in, "valid": valid_rows,
            "within_5m": within_grace, "late_quarantined": len(late_records),
            "quality_failed": quality_failed, "conservation_ok": conservation,
            "watermark_epoch": watermark,
        })
        if late_records:
            print(f"[LATE] batch={batch_id} quarantined={len(late_records)} rows in SQLite late_events", flush=True)
    finally:
        batch_df.unpersist()


(
    parse_events()
    .writeStream.queryName("hourly").foreachBatch(process_microbatch)
    .option("checkpointLocation", f"{CHECKPOINT}/hourly")
    .start()
)

print(
    "Full deep-dive consumer started "
    f"(windows=1h, event-time-grace={WATERMARK_MINUTES}m, "
    f"results={RESULTS}, late=SQLite:late_events, checkpoint={CHECKPOINT})",
    flush=True,
)
stage_event("consumer", "started", {
    "windows": "1h", "watermark_minutes": WATERMARK_MINUTES,
    "watermark_kind": "event-time window-end grace",
})
spark.streams.awaitAnyTermination()
