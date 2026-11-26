"""
Deep-dive check: batch hourly counts vs SQLite hourly_counts.

Uses the same slice as the producer (year range + earliest N rows).
Late injects are older test events beyond the watermark, so stream hourly totals
should match this batch baseline. late_ratings/ is reported separately.
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_unixtime, sum as spark_sum, window
from pyspark.sql.types import TimestampType

parser = argparse.ArgumentParser()
parser.add_argument("--start-year", type=int, default=1996)
parser.add_argument("--end-year", type=int, default=2023)
parser.add_argument("--max-messages", type=int, default=100)
args = parser.parse_args()

spark = (
    SparkSession.builder.appName("DeepDiveCompare")
    .master("local[2]")
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

from database import connect
with connect() as db:
    stored = [tuple(r) for r in db.execute("SELECT hour_start,movie_id,rating_count FROM hourly_counts")]
if not stored:
    raise SystemExit("No hourly results in database; run the consumer first")
stream = (spark.createDataFrame(stored, "hour_start string, movieId long, stream_count long")
          .withColumn("hour_start", col("hour_start").cast("timestamp")))

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

failed = mismatches.count() > 0 or extra_stream.count() > 0
spark.stop()
raise SystemExit(1 if failed else 0)
