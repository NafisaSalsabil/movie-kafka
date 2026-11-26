"""Exercise the real HTTP handler; Kafka is mocked only for parser-level tests."""
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

from database import connect, load_batch
from serve import Handler

class HttpTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.patch = patch('database.DB_PATH', self.temp.name + '/db.sqlite3')
        self.patch.start()
        load_batch([(1, 'Toy')], [(1, 4.5, 1000)], [(2020, 'Drama', 1)])
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_port)
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.patch.stop()
        self.temp.cleanup()
    def request(self, path, data=None, headers=None):
        body = data.encode() if isinstance(data, str) else data
        try:
            response = urlopen(Request(self.base + path, data=body, headers=headers or {}))
        except HTTPError as exc:
            response = exc
        return response.status, response.read(), response.headers
    def test_routes_and_response_headers(self):
        for route in ('/health', '/questions/1?test=1', '/questions/2', '/questions/3', '/demo/status'):
            status, body, headers = self.request(route)
            self.assertEqual(status, 200, (route, body))
            json.loads(body)
            self.assertEqual(headers['Cache-Control'], 'no-store')
        self.assertEqual(self.request('/questions/4')[0], 404)
        status, body, headers = self.request('/demo')
        self.assertEqual(status, 200)
        self.assertIn(b'Enter a bad row by hand', body)
        self.assertIn('text/html', headers['Content-Type'])
    def test_readiness_and_empty_stream(self):
        self.assertEqual(json.loads(self.request('/questions/3')[1])['status'], 'no_results_yet')
        with connect() as db:
            db.execute("DELETE FROM metadata WHERE key='batch_ready'")
        self.assertEqual(self.request('/questions/1')[0], 503)
    def test_invalid_http_payloads_do_not_reach_kafka(self):
        payloads = ['{', '[]', '{}',
            '{"userId":1,"movieId":1,"rating":"nine","timestamp":100}',
            '{"userId":true,"movieId":1,"rating":9,"timestamp":100}',
            '{"userId":1,"movieId":1,"rating":NaN,"timestamp":100}',
            'x' * 4097]
        with patch('serve.send_rating') as send:
            for value in payloads:
                self.assertEqual(self.request('/demo/inject', value)[0], 400, value)
            send.assert_not_called()
    def test_manual_bad_rating_is_acknowledged_then_checked_by_consumer(self):
        row = dict(userId=99, movieId=1, rating=9.0, timestamp=100)
        with patch('serve.send_rating', return_value={'offset': 12}) as send:
            code, body, _ = self.request('/demo/inject', json.dumps(row))
            self.assertEqual(code, 202)
            self.assertEqual(json.loads(body)['offset'], 12)
            send.assert_called_once_with(row)
    def test_foreign_origin_rejected(self):
        with patch('serve.send_rating') as send:
            self.assertEqual(self.request('/demo/inject', '{}', {'Origin':'https://elsewhere.example'})[0], 403)
            send.assert_not_called()
    def test_query_error_returns_json(self):
        with patch('serve.answer', side_effect=ValueError('private diagnostic')):
            code, body, _ = self.request('/questions/1')
            self.assertEqual(code, 500)
            self.assertNotIn(b'private diagnostic', body)
