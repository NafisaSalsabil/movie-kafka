"""
Deep-dive check: batch hourly counts vs stream_hourly_counts/.

Uses the same slice as the producer (year range + earliest N rows).
Late injects are duplicates beyond the watermark, so stream hourly totals
should match this batch baseline. late_ratings/ is reported separately.
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_unixtime, sum as spark_sum, window
from pyspark.sql.types import TimestampType

parser = argparse.ArgumentParser()
parser.add_argument("--start-year", type=int, default=1995)
parser.add_argument("--end-year", type=int, default=1996)
parser.add_argument("--max-messages", type=int, default=100)
args = parser.parse_args()

spark = (
    SparkSession.builder.appName("DeepDiveCompare")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    # Tolerate overwrite races if the stream consumer is still running
    .config("spark.sql.files.ignoreMissingFiles", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

ratings = (
    spark.read.parquet("ratings.parquet")
    .filter((col("year") >= args.start_year) & (col("year") <= args.end_year))
    .orderBy("timestamp")
    .limit(args.max_messages)
    .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))
)

batch = (
    ratings.groupBy(window(col("event_time"), "1 hour"), col("movieId"))
    .agg(count("*").alias("rating_count"))
    .select(
        col("window.start").alias("hour_start"),
        col("movieId"),
        col("rating_count").alias("batch_count"),
    )
)

if not os.path.isdir("stream_hourly_counts"):
    raise SystemExit("stream_hourly_counts/ missing — run the stream consumer first")

stream = (
    spark.read.parquet("stream_hourly_counts")
    # Partition cols may be strings on read; align types with the batch side
    .withColumn("hour_start", col("hour_start").cast("timestamp"))
    .withColumn("movieId", col("movieId").cast("long"))
    .groupBy("hour_start", "movieId")
    .agg(spark_sum("rating_count").alias("stream_count"))
)

batch = batch.withColumn("hour_start", col("hour_start").cast("timestamp")).withColumn(
    "movieId", col("movieId").cast("long")
)

# Compare only keys that exist in the batch slice (ignore extra late-hour
# keys created by the late-inject demo).
joined = (
    batch.join(stream, ["hour_start", "movieId"], "left")
    .fillna(0, ["stream_count"])
    .withColumn("diff", col("stream_count") - col("batch_count"))
)

mismatches = joined.filter(col("diff") != 0)
extra_stream = stream.join(batch, ["hour_start", "movieId"], "left_anti")

print(f"Batch ratings used: {ratings.count()}", flush=True)
print(f"Keys compared: {joined.count()}", flush=True)
print(f"Mismatched keys: {mismatches.count()}", flush=True)
print(f"Extra stream keys (e.g. late inject): {extra_stream.count()}", flush=True)
if mismatches.count() > 0:
    mismatches.orderBy(col("diff").desc()).show(50, truncate=False)
else:
    print("OK: stream hourly counts match batch baseline.", flush=True)

late_n = 0
if os.path.isdir("late_ratings"):
    late_n = spark.read.json("late_ratings").count()
print(f"Late ratings logged: {late_n}", flush=True)

spark.stop()
