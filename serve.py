"""Small, local HTTP query service; uses only the Python standard library."""
import json
import os
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from queries import answer
from observability import snapshot


def send_rating(row):
    """Deliberately bypass producer checks so the real consumer checks the manual row."""
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                             value_serializer=lambda value: json.dumps(value).encode(), acks='all',
                             max_block_ms=10000, request_timeout_ms=10000)
    try:
        receipt = producer.send('ratings', value=row).get(timeout=15)
        return dict(topic=receipt.topic, partition=receipt.partition, offset=receipt.offset, row=row,
                    status='acknowledged_by_kafka', next='Wait for the consumer quality counts and quarantine sample.')
    finally:
        producer.close(timeout=5)


def demo_status():
    result = snapshot()
    result['artifacts'] = {}
    for name in ('storage_sizes', 'restart_validation', 'validation', 'recording_index_benchmark'):
        file = Path('reports') / (name + '.json')
        if file.exists():
            try:
                result['artifacts'][name] = json.loads(file.read_text())
            except (ValueError, OSError):
                pass
    return result

class Handler(BaseHTTPRequestHandler):
    def respond(self, status, payload, content_type='application/json'):
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        route = urlsplit(self.path).path
        if route in ('/', '/demo'):
            self.respond(200, Path(__file__).with_name('demo.html').read_bytes(), 'text/html; charset=utf-8')
            return
        if route == "/health":
            status, payload = 200, {"status":"up"}
        elif route == '/demo/status':
            try:
                status, payload = 200, demo_status()
            except Exception as exc:
                status, payload = 503, {'error': str(exc)}
        elif route in ("/questions/1", "/questions/2", "/questions/3"):
            try:
                payload = answer(int(route[-1]))
                status = 200
            except RuntimeError as exc:
                status, payload = 503, {"error":str(exc)}
            except Exception:
                self.log_error("Database query failed")
                status, payload = 500, {"error":"Database query failed; see server logs"}
        else:
            status, payload = 404, {"routes":["/demo","/demo/status","/health","/questions/1","/questions/2","/questions/3"]}
        self.respond(status, payload)

    def do_POST(self):
        if self.path != '/demo/inject':
            self.respond(404, {'error': 'Unknown route'})
            return
        if self.headers.get('Origin') not in (None, 'http://' + self.headers.get('Host', '')):
            self.respond(403, {'error': 'Use the local dashboard to enter a row'})
            return
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 4096:
                raise ValueError('Enter a JSON row of at most 4096 bytes')
            row = json.loads(self.rfile.read(size), parse_constant=lambda value: (_ for _ in ()).throw(ValueError("Use finite JSON numbers")))
            required = {'userId', 'movieId', 'rating', 'timestamp'}
            if not isinstance(row, dict) or set(row) != required:
                raise ValueError('Use exactly userId, movieId, rating and timestamp')
            for field, value in row.items():
                if value is not None and (type(value) not in (int, float) or
                                          (field != 'rating' and type(value) is not int)):
                    raise ValueError('IDs/timestamp must be integers; rating must be numeric; null is allowed')
            self.respond(202, send_rating(row))
        except (ValueError, TypeError) as exc:
            self.respond(400, {'error': str(exc)})
        except Exception as exc:
            self.respond(503, {'error': str(exc)})

if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0",8000),Handler).serve_forever()
