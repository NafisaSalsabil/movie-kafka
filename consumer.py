"""
Part 2 full deep dive: Spark Structured Streaming consumer.

- 1-hour event-time windows
- 15-minute watermark
- Late ratings appended to late_ratings/ (and printed)
- Hourly counts written idempotently by (hour_start, movieId) to stream_hourly_counts/
- Checkpoints under checkpoints/stream_hourly/ for safe restart
"""

import os
import time
from datetime import datetime, timedelta

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    from_unixtime,
    lit,
    max as spark_max,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from quality import BAD_DIR, alert_if_needed, check_ratings_spark, stage

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT = "checkpoints/stream_hourly"
RESULTS = "stream_hourly_counts"
LATE_DIR = "late_ratings"
STATE_FILE = "late_state/max_event_time.txt"
WATERMARK_MINUTES = 15

schema = StructType(
    [
        StructField("userId", LongType(), False),
        StructField("movieId", LongType(), False),
        StructField("rating", DoubleType(), False),
        StructField("timestamp", LongType(), False),
    ]
)

spark = (
    SparkSession.builder.appName("MovieLensStreamDeepDive")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8",
    )
    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def ensure_ratings_topic():
    """Spark fails if the topic does not exist yet; create it before streaming."""
    for attempt in range(30):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA, client_id="ratings-init")
            try:
                admin.create_topics(
                    [NewTopic("ratings", num_partitions=1, replication_factor=1)]
                )
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
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(from_json(col("value").cast("string"), schema).alias("r"))
        .select("r.*")
        .withColumn(
            "event_time",
            from_unixtime(col("timestamp")).cast(TimestampType()),
        )
    )


def rating_events():
    # Part 4: only valid ratings enter windows / late logic
    return parse_events().filter(
        col("userId").isNotNull()
        & col("movieId").isNotNull()
        & col("rating").isNotNull()
        & col("timestamp").isNotNull()
        & (col("rating") >= 0.5)
        & (col("rating") <= 5.0)
        & (col("userId") > 0)
        & (col("movieId") > 0)
        & (col("timestamp") > 0)
    )


# Part 4: monitor raw Kafka parses — quarantine bad rows, count in/out
def monitor_quality(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # drop helper col before checks
    rows = batch_df.drop("event_time")
    good, bad, _fails = check_ratings_spark(rows)
    bad_n = bad.count()
    good_n = good.count()
    stage(f"consumer-batch-{batch_id}", good_n + bad_n, good_n, bad_n)
    alert_if_needed(bad_n, good_n + bad_n, f"consumer-batch-{batch_id}")
    if bad_n > 0:
        bad.write.mode("append").parquet(BAD_DIR)


(
    parse_events()
    .writeStream.foreachBatch(monitor_quality)
    .option("checkpointLocation", f"{CHECKPOINT}/quality")
    .start()
)


def load_max_et():
    if not os.path.exists(STATE_FILE):
        return None
    text = open(STATE_FILE, encoding="utf-8").read().strip()
    return datetime.fromisoformat(text) if text else None


def save_max_et(value):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    open(STATE_FILE, "w", encoding="utf-8").write(value.isoformat())


# --- Late log (durable) ---
late_state = {"max_et": load_max_et()}


def log_late(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    batch_max = batch_df.agg(spark_max("event_time")).collect()[0][0]

    if late_state["max_et"] is not None:
        threshold = late_state["max_et"] - timedelta(minutes=WATERMARK_MINUTES)
        late = batch_df.filter(col("event_time") < lit(threshold))
        n = late.count()
        if n > 0:
            (
                late.select("userId", "movieId", "rating", "timestamp", "event_time")
                .write.mode("append")
                .json(LATE_DIR)
            )
            print(f"[LATE] batch={batch_id} count={n} -> {LATE_DIR}/", flush=True)
            late.select("userId", "movieId", "rating", "event_time").show(
                n, truncate=False
            )
        else:
            print(f"[LATE] batch={batch_id} count=0", flush=True)

    if batch_max is not None and (
        late_state["max_et"] is None or batch_max > late_state["max_et"]
    ):
        late_state["max_et"] = batch_max
        save_max_et(batch_max)


(
    rating_events()
    .writeStream.foreachBatch(log_late)
    .option("checkpointLocation", f"{CHECKPOINT}/late")
    .start()
)

# --- Hourly counts (idempotent by hour_start + movieId) ---
hourly = (
    rating_events()
    .withWatermark("event_time", f"{WATERMARK_MINUTES} minutes")
    .groupBy(window(col("event_time"), "1 hour"), col("movieId"))
    .agg(count("*").alias("rating_count"))
)


def write_hourly(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[HOURLY] batch={batch_id} empty", flush=True)
        return

    out = batch_df.select(
        col("window.start").alias("hour_start"),
        col("window.end").alias("hour_end"),
        col("movieId"),
        col("rating_count"),
    )
    # Overwrite only partitions present in this batch → same key replaces count
    (
        out.write.mode("overwrite")
        .partitionBy("hour_start", "movieId")
        .parquet(RESULTS)
    )
    print(f"[HOURLY] batch={batch_id} updated_keys={out.count()} -> {RESULTS}/", flush=True)
    out.orderBy(col("rating_count").desc()).show(10, truncate=False)


(
    hourly.writeStream.outputMode("update")
    .foreachBatch(write_hourly)
    .option("checkpointLocation", f"{CHECKPOINT}/hourly")
    .start()
)

print(
    "Full deep-dive consumer started "
    f"(windows=1h, watermark={WATERMARK_MINUTES}m, "
    f"results={RESULTS}, late={LATE_DIR}, checkpoint={CHECKPOINT})",
    flush=True,
)
spark.streams.awaitAnyTermination()
