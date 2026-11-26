import json
import math
import tempfile
import unittest
from database import connect
from observability import record_quality, record_progress, snapshot, stage_event

class ObservabilityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = self.temp.name + '/db.sqlite3'
    def tearDown(self):
        self.temp.cleanup()
    def test_retry_does_not_double_quality_or_progress_totals(self):
        checks = dict(missing=0, range=1, ids=0, duplicates=0)
        for _ in range(2):
            record_quality('stream', 7, 3, 2, checks,
                           [{'rating':9, '_kafka_offset':12}], self.path)
            record_progress(dict(name='hourly',batchId=7,
                stateOperators=[dict(numRowsDroppedByWatermark=5)]), self.path)
        result = snapshot(self.path)
        self.assertEqual(result['quality']['stream']['rows_in'], 3)
        self.assertEqual(result['quality']['stream']['failed'], 1)
        self.assertEqual(result['watermark_dropped_total'], 5)
        self.assertEqual(len(result['quarantine_samples']), 1)
        self.assertEqual(result['quarantine_samples'][0]['row']['_kafka_offset'], 12)
    def test_nan_quarantine_can_be_encoded_as_strict_json(self):
        record_quality('stream', 0, 1, 0, dict(missing=0, range=1, ids=0, duplicates=0),
                       [{'rating':float('nan')}], self.path)
        payload = snapshot(self.path)
        json.dumps(payload, allow_nan=False)
        self.assertEqual(payload['quarantine_samples'][0]['row']['rating'], 'nan')
    def test_stage_event_persists_details(self):
        stage_event('batch', 'complete', {'ratings':1000000}, self.path)
        self.assertEqual(snapshot(self.path)['stages'][0]['detail']['ratings'], 1000000)
