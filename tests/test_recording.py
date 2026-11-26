import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch
from http.server import ThreadingHTTPServer

from database import load_batch
from observability import record_progress, record_quality, snapshot
from serve import Handler


class RecordingTest(unittest.TestCase):
    def test_retried_quality_batch_is_not_counted_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'demo.sqlite3')
            checks = dict(missing=0, range=1, ids=0, duplicates=0)
            for _ in range(2):
                record_quality('stream', 7, 2, 1, checks, [dict(rating=9.0, _kafka_offset=42)], path)
            record_progress(dict(name='hourly', batchId=3, stateOperators=[dict(numRowsDroppedByWatermark=5)]), path)
            d = snapshot(path)
            self.assertEqual(d['quality']['stream']['rows_in'], 2)
            self.assertEqual(d['quality']['stream']['failed'], 1)
            self.assertEqual(d['quarantine_samples'][0]['row']['_kafka_offset'], 42)
            self.assertEqual(len(d['quarantine_samples']), 1)
            self.assertEqual(d['watermark_dropped_total'], 5)

    def test_http_manual_row_receipt_queries_and_input_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'demo.sqlite3')
            load_batch([(1, 'A')], [(1, 4.8, 1000)], [(2020, 'Drama', 1000)], path)
            with patch('database.DB_PATH', path):
                server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                base = 'http://127.0.0.1:' + str(server.server_port)
                try:
                    with urllib.request.urlopen(base + '/demo') as response:
                        self.assertIn(b'Hand-added bad row', response.read())
                    with urllib.request.urlopen(base + '/questions/1') as response:
                        result = json.load(response)
                        self.assertEqual(result['rows'][0]['title'], 'A')
                        self.assertIn('SELECT', result['sql'])
                    row = dict(userId=99, movieId=1, rating=9.0, timestamp=1697148000)
                    with patch('serve.send_rating', return_value=dict(offset=42, row=row)) as send:
                        request = urllib.request.Request(base + '/demo/inject', data=json.dumps(row).encode())
                        with urllib.request.urlopen(request) as response:
                            self.assertEqual(response.status, 202)
                            self.assertEqual(json.load(response)['offset'], 42)
                        send.assert_called_once_with(row)
                    for body, origin, code in [(b'[]', None, 400), (b'{}', None, 400),
                                                (json.dumps(row).encode(), 'https://foreign.example', 403)]:
                        request = urllib.request.Request(base + '/demo/inject', data=body,
                                                         headers={'Origin': origin} if origin else {})
                        with self.assertRaises(urllib.error.HTTPError) as error:
                            urllib.request.urlopen(request)
                        self.assertEqual(error.exception.code, code)
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join()
