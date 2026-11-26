"""Driver-side SQLite serving sink; no executor opens the database."""
import os
import json
import math
import sqlite3
from contextlib import contextmanager

DB_PATH = os.getenv("DATABASE_PATH", "data/analytics.sqlite3")
SCHEMA = """
CREATE TABLE IF NOT EXISTS movies(movie_id INTEGER PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS movie_stats(movie_id INTEGER PRIMARY KEY, avg_rating REAL NOT NULL, rating_count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS genre_year(year INTEGER NOT NULL, genre TEXT NOT NULL, rating_count INTEGER NOT NULL, PRIMARY KEY(year, genre));
CREATE TABLE IF NOT EXISTS hourly_counts(hour_start TEXT NOT NULL, hour_end TEXT NOT NULL, movie_id INTEGER NOT NULL, rating_count INTEGER NOT NULL, PRIMARY KEY(hour_start, movie_id));
CREATE TABLE IF NOT EXISTS sink_batches(stream TEXT NOT NULL, batch_id INTEGER NOT NULL, PRIMARY KEY(stream,batch_id));
CREATE TABLE IF NOT EXISTS pipeline_metrics(stage TEXT PRIMARY KEY, rows_in INTEGER NOT NULL, rows_out INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS query_timings(id INTEGER PRIMARY KEY, measured_at TEXT DEFAULT CURRENT_TIMESTAMP, question INTEGER NOT NULL, indexed INTEGER NOT NULL, elapsed_ms REAL NOT NULL, result_rows INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS quality_batches(stream TEXT NOT NULL, batch_id INTEGER NOT NULL, rows_in INTEGER NOT NULL, rows_out INTEGER NOT NULL, failed INTEGER NOT NULL, checks_json TEXT NOT NULL, PRIMARY KEY(stream,batch_id));
CREATE TABLE IF NOT EXISTS quality_samples(stream TEXT NOT NULL, batch_id INTEGER NOT NULL, sample_no INTEGER NOT NULL, row_json TEXT NOT NULL, PRIMARY KEY(stream,batch_id,sample_no));
CREATE TABLE IF NOT EXISTS stream_progress(name TEXT NOT NULL, batch_id INTEGER NOT NULL, progress_json TEXT NOT NULL, PRIMARY KEY(name,batch_id));
CREATE TABLE IF NOT EXISTS stage_events(id INTEGER PRIMARY KEY, observed_at TEXT DEFAULT CURRENT_TIMESTAMP, stage TEXT NOT NULL, state TEXT NOT NULL, detail_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS stream_batches(batch_id INTEGER PRIMARY KEY, rows_in INTEGER NOT NULL, valid_rows INTEGER NOT NULL, quality_failed INTEGER NOT NULL, within_grace INTEGER NOT NULL, late_rows INTEGER NOT NULL, max_event_time INTEGER, watermark INTEGER, committed_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS late_events(source_partition INTEGER NOT NULL, source_offset INTEGER NOT NULL, batch_id INTEGER NOT NULL, user_id INTEGER, movie_id INTEGER, rating REAL, event_timestamp INTEGER, window_start INTEGER NOT NULL, window_end INTEGER NOT NULL, watermark_at_arrival INTEGER NOT NULL, late_seconds INTEGER NOT NULL, row_json TEXT NOT NULL, PRIMARY KEY(source_partition,source_offset));
"""
INDEXES = {
    "idx_movie_ranking": "CREATE INDEX IF NOT EXISTS idx_movie_ranking ON movie_stats(avg_rating DESC, movie_id) WHERE rating_count >= 1000",
    "idx_genre_ranking": "CREATE INDEX IF NOT EXISTS idx_genre_ranking ON genre_year(year, rating_count DESC, genre)",
    "idx_hour_ranking": "CREATE INDEX IF NOT EXISTS idx_hour_ranking ON hourly_counts(hour_start DESC, rating_count DESC, movie_id)",
}

@contextmanager
def connect(path=None):
    path = path or DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    db = sqlite3.connect(path, timeout=60)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript(SCHEMA)
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def indexes(db):
    for sql in INDEXES.values():
        db.execute(sql)


def load_batch(movies, stats, genres, path=None):
    with connect(path) as db:
        db.execute("BEGIN IMMEDIATE")
        for table in ("movies", "movie_stats", "genre_year"):
            db.execute(f"DELETE FROM {table}")
        db.executemany("INSERT INTO movies VALUES (?,?)", movies)
        db.executemany("INSERT INTO movie_stats VALUES (?,?,?)", stats)
        db.executemany("INSERT INTO genre_year VALUES (?,?,?)", genres)
        for table in ("movies", "movie_stats", "genre_year"):
            count = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            db.execute("INSERT OR REPLACE INTO pipeline_metrics VALUES (?,?,?)", (f"database-{table}",count,count))
            print(f"[QUALITY] stage=database-{table} in={count} out={count} failed=0", flush=True)
        indexes(db)
        db.execute("INSERT OR REPLACE INTO metadata VALUES ('batch_ready','1')")


def load_hourly(rows, batch_id, path=None):
    """Replace absolute Spark counts atomically with a replay ledger."""
    with connect(path) as db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute("SELECT 1 FROM sink_batches WHERE stream='hourly' AND batch_id=?", (batch_id,)).fetchone():
            return 0
        count = 0
        for start, end, movie_id, rating_count in rows:
            db.execute("""INSERT INTO hourly_counts VALUES (?,?,?,?)
                ON CONFLICT(hour_start,movie_id) DO UPDATE SET
                hour_end=excluded.hour_end, rating_count=excluded.rating_count""",
                (str(start), str(end), int(movie_id), int(rating_count)))
            count += 1
        db.execute("INSERT INTO sink_batches VALUES ('hourly',?)", (batch_id,))
        db.execute("INSERT OR REPLACE INTO pipeline_metrics VALUES (?,?,?)", (f"database-hourly-{batch_id}",count,count))
        indexes(db)
    print(f"[QUALITY] stage=database-hourly-{batch_id} in={count} out={count} failed=0", flush=True)
    return count


def stream_event_time_state(path=None):
    """Return (maximum valid event timestamp, committed batch IDs)."""
    with connect(path) as db:
        row = db.execute("SELECT value FROM metadata WHERE key='stream_max_event_time'").fetchone()
        return (int(row[0]) if row else None)


def commit_stream_batch(batch_id, rows_in, quality_failed, checks, within_rows,
                        late_rows, hourly_counts, max_event_time, samples=(), path=None):
    """Atomically store one microbatch, its late rows, counts, and replay marker.

    `within_rows + late_rows + quality_failed == rows_in` is enforced before any
    writes. Hourly counts are deltas because a grace-period arrival can revise
    an already stored event-time window.
    """
    rows_in = int(rows_in)
    quality_failed = int(quality_failed)
    within_rows = int(within_rows)
    late_rows = list(late_rows)
    hourly_counts = list(hourly_counts)
    late_count = len(late_rows)
    valid_rows = within_rows + late_count
    if within_rows < 0 or quality_failed < 0 or rows_in != valid_rows + quality_failed:
        raise ValueError("stream row conservation failed: incoming != within grace + late + quality failures")
    if sum(int(row[3]) for row in hourly_counts) != within_rows:
        raise ValueError("hourly aggregate counts do not equal within-grace input rows")

    with connect(path) as db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute("SELECT 1 FROM stream_batches WHERE batch_id=?", (int(batch_id),)).fetchone():
            return {"replayed": True, "within_grace": 0, "late_rows": 0}

        previous = db.execute("SELECT value FROM metadata WHERE key='stream_max_event_time'").fetchone()
        previous_max = int(previous[0]) if previous else None
        current_max = max_event_time if previous_max is None else previous_max
        if max_event_time is not None:
            current_max = max(int(max_event_time), previous_max or int(max_event_time))
        watermark = None if current_max is None else int(current_max) - 300

        db.executemany("""INSERT INTO hourly_counts(hour_start,hour_end,movie_id,rating_count)
            VALUES (?,?,?,?) ON CONFLICT(hour_start,movie_id) DO UPDATE SET
            hour_end=excluded.hour_end,
            rating_count=hourly_counts.rating_count + excluded.rating_count""",
            hourly_counts)
        db.executemany("""INSERT INTO late_events
            (source_partition,source_offset,batch_id,user_id,movie_id,rating,event_timestamp,
             window_start,window_end,watermark_at_arrival,late_seconds,row_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            ((int(row["_kafka_partition"]), int(row["_kafka_offset"]), int(batch_id),
              row.get("userId"), row.get("movieId"), row.get("rating"), row.get("timestamp"),
              int(row["_window_start"]), int(row["_window_end"]), int(row["_watermark_at_arrival"]),
              int(row["_late_seconds"]), row["_row_json"]) for row in late_rows))

        db.execute("INSERT INTO stream_batches VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                   (int(batch_id), rows_in, valid_rows, quality_failed, within_rows,
                    late_count, current_max, watermark))
        db.execute("INSERT OR REPLACE INTO sink_batches VALUES ('hourly',?)", (int(batch_id),))
        db.execute("INSERT OR REPLACE INTO quality_batches VALUES ('stream',?,?,?,?,?)",
                   (int(batch_id), rows_in, valid_rows, quality_failed, __import__('json').dumps(checks)))
        db.execute("DELETE FROM quality_samples WHERE stream='stream' AND batch_id=?", (int(batch_id),))
        def safe(value):
            if isinstance(value, float) and not math.isfinite(value):
                return str(value)
            if isinstance(value, dict):
                return {key: safe(item) for key, item in value.items()}
            if isinstance(value, (tuple, list)):
                return [safe(item) for item in value]
            return value
        db.executemany("INSERT INTO quality_samples VALUES ('stream',?,?,?)",
                       ((int(batch_id), n, json.dumps(safe(row), default=str, allow_nan=False))
                        for n, row in enumerate(samples)))
        db.execute("INSERT OR REPLACE INTO pipeline_metrics VALUES (?,?,?)",
                   (f"database-hourly-{batch_id}", valid_rows, within_rows))
        if current_max is not None:
            db.execute("INSERT OR REPLACE INTO metadata VALUES ('stream_max_event_time',?)",
                       (str(current_max),))
        indexes(db)

    print(f"[QUALITY] stage=consumer-batch-{batch_id} in={rows_in:,} out={within_rows:,} failed={rows_in-within_rows:,}", flush=True)
    return {"replayed": False, "within_grace": within_rows, "late_rows": late_count,
            "quality_failed": quality_failed, "watermark": watermark}
