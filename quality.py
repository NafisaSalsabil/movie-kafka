"""
Part 4: quality checks + stage row counts.

Four checks (each reports how many rows failed):
  1. missing  — nulls in required fields
  2. range    — rating outside [0.5, 5.0]
  3. ids      — userId/movieId/timestamp <= 0
  4. duplicates — extra rows with same (userId, movieId, timestamp)

Bad rows are quarantined (not dropped silently). Alert if fail rate > 1%.
"""

import math

from pyspark.sql import functions as F
from pyspark.sql.window import Window

ALERT_FRAC = 0.01
BAD_DIR = "bad_ratings"


def stage(name, rows_in, rows_out, failed=0):
    print(
        f"[QUALITY] stage={name} in={rows_in:,} out={rows_out:,} failed={failed:,}",
        flush=True,
    )


def alert_if_needed(failed, total, name):
    if total > 0 and failed / total > ALERT_FRAC:
        print(
            f"[ALERT] stage={name} fail_rate={failed / total:.2%} "
            f"exceeds {ALERT_FRAC:.0%}",
            flush=True,
        )


def check_ratings_spark(df):
    """Return (good_df, bad_df, {check_name: fail_count})."""
    null_cond = (
        F.col("userId").isNull()
        | F.col("movieId").isNull()
        | F.col("rating").isNull()
        | F.col("timestamp").isNull()
    )
    range_cond = (F.col("rating") < 0.5) | (F.col("rating") > 5.0)
    id_cond = (
        (F.col("userId") <= 0)
        | (F.col("movieId") <= 0)
        | (F.col("timestamp") <= 0)
    )

    # Prefer a valid row when an invalid row shares its duplicate key.
    base_bad = (null_cond | F.coalesce(range_cond, F.lit(False))
                | F.coalesce(id_cond, F.lit(False)))
    w = Window.partitionBy("userId", "movieId", "timestamp").orderBy(
        base_bad.cast("int"), F.monotonically_increasing_id()
    )
    ranked = df.withColumn("_rn", F.row_number().over(w))
    dup_cond = F.col("_rn") > 1
    conditions = {"missing": null_cond, "range": range_cond,
                  "ids": id_cond, "duplicates": dup_cond}
    counts = ranked.agg(*[
        F.sum(F.when(condition, 1).otherwise(0)).alias(name)
        for name, condition in conditions.items()
    ]).first()
    fails = {name: int(counts[name] or 0) for name in conditions}
    for name, n in fails.items():
        print(f"[QUALITY] check={name} failed={n:,}", flush=True)

    bad_cond = null_cond | F.coalesce(range_cond, F.lit(False)) | F.coalesce(id_cond, F.lit(False)) | dup_cond
    bad = ranked.filter(bad_cond).drop("_rn")
    good = ranked.filter(~bad_cond).drop("_rn")
    return good, bad, fails


def row_failures(row, seen=None):
    """Validate a producer dict without crashing on malformed numeric values."""
    failed = []
    required = ("userId", "movieId", "rating", "timestamp")
    if any(row.get(key) is None for key in required):
        failed.append("missing")
    value = row.get("rating")
    if value is not None and (
        type(value) not in (int, float) or not math.isfinite(value)
        or not 0.5 <= value <= 5.0
    ):
        failed.append("range")
    ids = [row.get(key) for key in ("userId", "movieId", "timestamp")]
    if any(value is not None and (type(value) is not int or value <= 0) for value in ids):
        failed.append("ids")
    if seen is not None and not failed:
        key = tuple(ids)
        if key in seen:
            failed.append("duplicates")
        else:
            seen.add(key)
    return failed
