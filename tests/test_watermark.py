import os
import tempfile
import unittest

from database import commit_stream_batch, connect
from event_watermark import GRACE_SECONDS, is_beyond_grace, window_bounds, watermark_epoch
from observability import snapshot


class EventWatermarkTest(unittest.TestCase):
    def test_five_minute_boundary_is_inclusive(self):
        event = 10 * 3600 + 15
        start, end = window_bounds(event)
        self.assertEqual(end - start, 3600)
        self.assertFalse(is_beyond_grace(event, end + GRACE_SECONDS))
        self.assertTrue(is_beyond_grace(event, end + GRACE_SECONDS + 1))
        self.assertEqual(watermark_epoch(end + GRACE_SECONDS), end)

    def test_rows_are_counted_included_or_quarantined_and_replay_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "analytics.sqlite3")
            first_event = 11 * 3600 + 5 * 60 + 1
            first_start, first_end = window_bounds(first_event)
            commit_stream_batch(
                batch_id=0, rows_in=1, quality_failed=0,
                checks={"missing": 0, "range": 0, "ids": 0, "duplicates": 0},
                within_rows=1, late_rows=[],
                hourly_counts=[("1970-01-01 11:00:00", "1970-01-01 12:00:00", 7, 1)],
                max_event_time=first_event, path=path,
            )

            late_timestamp = 10 * 3600 + 5 * 60 + 1
            late_start, late_end = window_bounds(late_timestamp)
            late = {
                "userId": 2, "movieId": 7, "rating": 4.5, "timestamp": late_timestamp,
                "_kafka_partition": 0, "_kafka_offset": 42,
                "_window_start": late_start, "_window_end": late_end,
                "_watermark_at_arrival": first_event - GRACE_SECONDS,
                "_late_seconds": 1,
                "_row_json": '{"userId":2,"movieId":7,"rating":4.5}',
            }
            args = dict(
                batch_id=1, rows_in=1, quality_failed=0,
                checks={"missing": 0, "range": 0, "ids": 0, "duplicates": 0},
                within_rows=0, late_rows=[late], hourly_counts=[],
                max_event_time=first_event, path=path,
            )
            committed = commit_stream_batch(**args)
            self.assertEqual(committed["late_rows"], 1)
            self.assertTrue(commit_stream_batch(**args)["replayed"])

            with connect(path) as db:
                stored = db.execute("SELECT source_offset,late_seconds,row_json FROM late_events").fetchone()
                self.assertEqual((stored["source_offset"], stored["late_seconds"]), (42, 1))
                self.assertEqual(db.execute("SELECT SUM(rating_count) FROM hourly_counts").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM stream_batches").fetchone()[0], 2)

            counts = snapshot(path)["late_arrivals"]
            self.assertEqual(counts["rows_in"], 2)
            self.assertEqual(counts["within_grace"], 1)
            self.assertEqual(counts["quarantined_late"], 1)
            self.assertEqual(counts["late_event_rows"], 1)
            self.assertTrue(counts["conservation_ok"])

    def test_count_mismatch_rolls_back_before_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "analytics.sqlite3")
            with self.assertRaisesRegex(ValueError, "row conservation"):
                commit_stream_batch(
                    batch_id=0, rows_in=3, quality_failed=0,
                    checks={"missing": 0}, within_rows=2, late_rows=[],
                    hourly_counts=[("2020-01-01 00:00:00", "2020-01-01 01:00:00", 1, 2)],
                    max_event_time=1, path=path,
                )
            with connect(path) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM stream_batches").fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM hourly_counts").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
