"""Durable, replay-safe recording evidence from the actual pipeline."""
import json
import math
from database import connect


def json_safe(value):
    """Keep rejected NaN/Infinity values visible without emitting invalid JSON."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def stage_event(stage, state, detail=None, path=None):
    with connect(path) as db:
        db.execute("INSERT INTO stage_events(stage,state,detail_json) VALUES (?,?,?)",
                   (stage, state, json.dumps(detail or {})))


def record_quality(stream, batch_id, rows_in, rows_out, checks, samples=(), path=None):
    with connect(path) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT OR REPLACE INTO quality_batches VALUES (?,?,?,?,?,?)",
                   (stream, batch_id, rows_in, rows_out, rows_in - rows_out, json.dumps(checks)))
        db.execute("DELETE FROM quality_samples WHERE stream=? AND batch_id=?", (stream, batch_id))
        db.executemany("INSERT INTO quality_samples VALUES (?,?,?,?)",
                       ((stream, batch_id, n, json.dumps(json_safe(row), default=str, allow_nan=False)) for n, row in enumerate(samples)))


def record_progress(progress, path=None):
    with connect(path) as db:
        db.execute("INSERT OR REPLACE INTO stream_progress VALUES (?,?,?)",
                   (progress['name'], progress['batchId'], json.dumps(progress)))


def snapshot(path=None):
    with connect(path) as db:
        counts = {table: db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                  for table in ('movies', 'movie_stats', 'genre_year', 'hourly_counts', 'sink_batches')}
        counts['stream_ratings'] = db.execute('SELECT COALESCE(SUM(rating_count),0) FROM hourly_counts').fetchone()[0]
        counts['batch_ready'] = bool(db.execute("SELECT 1 FROM metadata WHERE key='batch_ready'").fetchone())
        quality = [dict(r) for r in db.execute('SELECT * FROM quality_batches ORDER BY stream,batch_id')]
        totals = {}
        for row in quality:
            row['checks'] = json.loads(row.pop('checks_json'))
            total = totals.setdefault(row['stream'], dict(rows_in=0, rows_out=0, failed=0,
                                                         checks=dict(missing=0, range=0, ids=0, duplicates=0)))
            for key in ('rows_in', 'rows_out', 'failed'):
                total[key] += row[key]
            for key, value in row['checks'].items():
                total['checks'][key] += value
        samples = [dict(r) for r in db.execute('SELECT * FROM quality_samples ORDER BY rowid DESC LIMIT 20')]
        for row in samples:
            row['row'] = json.loads(row.pop('row_json'))
        progress = {}
        watermark_dropped_total = 0
        for row in db.execute('SELECT * FROM stream_progress ORDER BY batch_id'):
            progress[row['name']] = json.loads(row['progress_json'])
        batch_totals = db.execute('''SELECT COALESCE(SUM(rows_in),0) rows_in,
            COALESCE(SUM(valid_rows),0) valid_rows,
            COALESCE(SUM(quality_failed),0) quality_failed,
            COALESCE(SUM(within_grace),0) within_grace,
            COALESCE(SUM(late_rows),0) late_rows,
            MAX(watermark) watermark FROM stream_batches''').fetchone()
        late_event_rows = db.execute('SELECT COUNT(*) FROM late_events').fetchone()[0]
        late_samples = [dict(row) for row in db.execute('''SELECT source_partition,source_offset,
            event_timestamp,window_start,window_end,watermark_at_arrival,late_seconds,row_json
            FROM late_events ORDER BY source_partition DESC,source_offset DESC LIMIT 20''')]
        for row in late_samples:
            row['row'] = json.loads(row.pop('row_json'))
        # Keep reading old captured Spark-watermark counters for historical
        # dashboards; the current consumer records its exact count in SQLite.
        spark_dropped = sum(
            op.get('numRowsDroppedByWatermark', 0)
            for p in progress.values()
            for op in p.get('stateOperators', [])
        )
        watermark_dropped_total = batch_totals['late_rows'] or spark_dropped
        late_arrivals = dict(
            rows_in=batch_totals['rows_in'], valid_rows=batch_totals['valid_rows'],
            quality_failed=batch_totals['quality_failed'], within_grace=batch_totals['within_grace'],
            quarantined_late=batch_totals['late_rows'], late_event_rows=late_event_rows,
            watermark_epoch=batch_totals['watermark'], samples=late_samples,
            conservation_ok=(batch_totals['rows_in'] == batch_totals['within_grace']
                             + batch_totals['late_rows'] + batch_totals['quality_failed']
                             and batch_totals['valid_rows'] == batch_totals['within_grace']
                             + batch_totals['late_rows'] and late_event_rows == batch_totals['late_rows']),
        )
        events = [dict(r) for r in db.execute('SELECT * FROM stage_events ORDER BY id DESC LIMIT 12')]
        for row in events:
            row['detail'] = json.loads(row.pop('detail_json'))
    return dict(database=counts, quality=totals, quality_batches=quality[-20:],
                quarantine_samples=samples, progress=progress, stages=events,
                watermark_dropped_total=watermark_dropped_total, late_arrivals=late_arrivals)
