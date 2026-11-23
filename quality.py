"""
Part 4: quality checks + stage row counts.

Four checks (each reports how many rows failed):
  1. missing  — nulls in required fields
  2. range    — rating outside [0.5, 5.0]
  3. ids      — userId/movieId/timestamp <= 0
  4. duplicates — extra rows with same (userId, movieId, timestamp)

Bad rows are quarantined (not dropped silently). Alert if fail rate > 1%.
"""

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

    w = Window.partitionBy("userId", "movieId", "timestamp").orderBy(
        F.monotonically_increasing_id()
    )
    ranked = df.withColumn("_rn", F.row_number().over(w))
    dup_cond = F.col("_rn") > 1

    fails = {
        "missing": df.filter(null_cond).count(),
        "range": df.filter(range_cond).count(),
        "ids": df.filter(id_cond).count(),
        "duplicates": ranked.filter(dup_cond).count(),
    }
    for name, n in fails.items():
        print(f"[QUALITY] check={name} failed={n:,}", flush=True)

    bad_cond = null_cond | range_cond | id_cond | dup_cond
    bad = ranked.filter(bad_cond).drop("_rn")
    good = ranked.filter(~bad_cond).drop("_rn")
    return good, bad, fails


def row_failures(row, seen):
    """Producer-side checks on one dict. Returns list of failed check names."""
    failed = []
    if (
        row.get("userId") is None
        or row.get("movieId") is None
        or row.get("rating") is None
        or row.get("timestamp") is None
    ):
        failed.append("missing")
    else:
        if not (0.5 <= float(row["rating"]) <= 5.0):
            failed.append("range")
        if int(row["userId"]) <= 0 or int(row["movieId"]) <= 0 or int(row["timestamp"]) <= 0:
            failed.append("ids")
        key = (row["userId"], row["movieId"], row["timestamp"])
        if key in seen:
            failed.append("duplicates")
        else:
            seen.add(key)
    return failed
