"""Run recording scenes in a separate Docker project. No state is deleted."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / 'recording-runtime'
ENV = dict(os.environ, KAFKA_HOST_PORT='19093', SERVE_HOST_PORT='8001')
PROJECT_FILE = RUNTIME / 'project.json'
PROJECT = json.loads(PROJECT_FILE.read_text())['project'] if PROJECT_FILE.exists() else 'movie-reviews-recording'
COMPOSE = ['docker', 'compose', '-p', PROJECT, '-f', str(ROOT / 'docker-compose.yml'),
           '-f', str(ROOT / 'compose.recording.yml')]
URL = 'http://localhost:8001'


def compose(*args, capture=False):
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, env=ENV, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def status():
    with urllib.request.urlopen(URL + '/demo/status', timeout=90) as response:
        return json.load(response)


def wait_for(predicate, timeout=600):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, ValueError):
            pass
        time.sleep(2)
    raise RuntimeError('Timed out waiting for the pipeline; inspect python3 demo.py logs')


def active_batch():
    """An offset log without a commit proves Spark has an unfinished input batch."""
    checkpoint = RUNTIME / 'checkpoints/stream_grace5/hourly'
    files = [p for p in (checkpoint / 'offsets').glob('*') if p.name.isdigit()]
    if not files:
        return None
    file = max(files, key=lambda p: int(p.name))
    if (checkpoint / 'commits' / file.name).exists():
        return None

    def offset(p):
        if not p.exists():
            return 0
        for line in p.read_text().splitlines():
            if line.startswith('{'):
                value = json.loads(line)
                if 'ratings' in value:
                    return sum(value['ratings'].values())
        return 0

    start = offset(file.with_name(str(int(file.name) - 1)))
    end = offset(file)
    return dict(batch_id=int(file.name), start_offset=start, end_offset=end,
                input_offsets=end - start) if end > start else None


def recovery(restarts):
    output = RUNTIME / 'reports/restart_validation.json'
    if output.exists():
        raise RuntimeError('This recording run already has restart evidence. Preserve it; use a fresh copy for another experiment.')
    result = dict(target_restarts=restarts, status='running', restarts=[],
                  method='SIGKILL consumer; preserve Kafka, SQLite and checkpoints. Require an unfinished nonempty hourly input batch for the first crash, a new Spark runId after each restart, then full CSV reconciliation.')

    def save():
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix('.tmp')
        temporary.write_text(json.dumps(result, indent=2))
        temporary.replace(output)

    save()
    try:
        for i in range(restarts):
            def candidate():
                d = status()
                if not d['database']['stream_ratings'] or 'hourly' not in d['progress']:
                    return None
                active = active_batch()
                producer = 'producer' in compose('ps', '--status', 'running', '--services', capture=True).splitlines()
                if i == 0 and (not active or not producer):
                    return None
                return d, active, producer

            before, active, producer_running = wait_for(candidate)
            # Recheck immediately before the signal: observing an earlier batch is insufficient.
            if i == 0:
                active = active_batch()
                if not active:
                    before, active, producer_running = wait_for(candidate)
            previous_run = before['progress']['hourly']['runId']
            entry = dict(restart=i + 1, producer_running=producer_running, active_batch=active,
                         before=dict(rating_total=before['database']['stream_ratings'],
                                     committed_batches=before['database']['sink_batches'],
                                     last_progress=before['progress']['hourly']), signal='SIGKILL')
            # Persist the intended crash before performing it, including failures or interruption.
            entry['status'] = 'crashing'
            result['restarts'].append(entry)
            save()
            started = time.monotonic()
            compose('kill', '-s', 'SIGKILL', 'consumer')
            compose('start', 'consumer')

            def recovered():
                d = status()
                p = d['progress'].get('hourly')
                return d if p and p['runId'] != previous_run else None

            after = wait_for(recovered)
            entry.update(status='recovered', recovery_seconds=round(time.monotonic() - started, 2),
                         after=dict(rating_total=after['database']['stream_ratings'],
                                    committed_batches=after['database']['sink_batches'],
                                    last_progress=after['progress']['hourly']))
            save()
            print(f"Restart {i+1}/{restarts}: {entry['recovery_seconds']} seconds; "
                  f"stored ratings {entry['before']['rating_total']:,} → {entry['after']['rating_total']:,}", flush=True)
        result['status'] = 'recovered_pending_csv_verification'
        save()
    except BaseException:
        result['status'] = 'incomplete'
        save()
        raise


def verify():
    def caught_up():
        d = status()
        source = d['artifacts'].get('storage_sizes', {}).get('ratings', {}).get('rows_out')
        finished = json.loads(compose('ps', '-a', '--format', 'json', 'producer', capture=True))
        # Compose emits one JSON object per service with this filter.
        return d if (finished.get('State') == 'exited' and finished.get('ExitCode') == 0
                     and source and d['database']['stream_ratings'] == source
                     and d['watermark_dropped_total'] >= 5) else None

    wait_for(caught_up, timeout=900)
    compose('run', '--rm', '--no-deps', 'batch', 'python3', '/app/verify_results.py')
    file = RUNTIME / 'reports/restart_validation.json'
    if file.exists():
        evidence = json.loads(file.read_text())
        complete = (len(evidence['restarts']) == evidence['target_restarts'] and
                    all(e.get('status') == 'recovered' for e in evidence['restarts']) and
                    evidence['restarts'][0].get('active_batch', {}).get('input_offsets', 0) > 0)
        if complete:
            evidence['status'] = 'passed_with_full_csv_reconciliation'
            evidence['csv_verification'] = json.loads((RUNTIME / 'reports/validation.json').read_text())['checks']
            file.write_text(json.dumps(evidence, indent=2))


def benchmark():
    # SQLite backup is taken inside Linux; never open the live Docker DB on the host.
    code = '''import json,sqlite3,statistics
from database import connect, indexes, INDEXES
from queries import answer
target='/tmp/recording-benchmark.sqlite3'
with connect() as db:
    copy=sqlite3.connect(target); db.backup(copy); copy.close()
results={}
try:
    for enabled in (False,True):
        with connect(target) as db:
            if enabled: indexes(db)
            else:
                for name in INDEXES: db.execute('DROP INDEX IF EXISTS '+name)
        results[str(enabled).lower()]={}
        for q in (1,2,3):
            answer(q,target,indexed=enabled)
            samples=[answer(q,target,indexed=enabled)['elapsed_ms'] for _ in range(30)]
            results[str(enabled).lower()][q]={'median_ms':statistics.median(samples),'n':30}
    report={'method':'SQLite backup snapshot inside Linux, one warmup then 30 trials, primary keys retained; sequential warm-cache measurements.', 'results':results}
    with open('reports/recording_index_benchmark.json','w') as f: json.dump(report,f,indent=2)
    print(json.dumps(report,indent=2))
finally:
    import os
    os.remove(target)
'''
    compose('exec', '-T', 'serve', 'python3', '-c', code)


def check():
    d = status()
    a = d['artifacts']
    recovery_evidence = a.get('restart_validation', {})
    v = a.get('validation', {})
    checks = dict(
        batch_complete=d['database']['batch_ready'],
        million_source_ratings=a.get('storage_sizes', {}).get('ratings', {}).get('rows_out', 0) >= 1000000,
        database_arrivals=d['database']['stream_ratings'] >= 1000000,
        consumer_range_failure=d['quality'].get('stream', {}).get('checks', {}).get('range', 0) >= 1,
        quarantined_range_sample=any(s['stream'] == 'stream' and
                                    isinstance(s['row'].get('rating'), (int, float)) and
                                    s['row']['rating'] > 5 for s in d['quarantine_samples']),
        approved_restart_count=(recovery_evidence.get('status') == 'passed_with_full_csv_reconciliation' and
                                len(recovery_evidence.get('restarts', [])) in (2, 3)),
        all_csv_checks=bool(v.get('checks')) and all(v['checks'].values()),
        five_late_drops=d['watermark_dropped_total'] >= 5,
        measured_index_comparison='recording_index_benchmark' in a,
    )
    answers = []
    for q in (1, 2, 3):
        with urllib.request.urlopen(URL + '/questions/' + str(q), timeout=30) as response:
            answers.append(json.load(response))
    checks['three_database_answers'] = all(a['status'] == 'ready' for a in answers)
    report = dict(checks=checks, software_ready=all(checks.values()), answers=answers,
                  manual_requirements=['Record 6–8 minutes of actual execution', 'Every team member speaks',
                                       'Type the bad row on camera', 'Submit video with report or TMU-accessible Drive link'])
    (RUNTIME / 'reports/recording_readiness.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    if not report['software_ready']:
        raise SystemExit('Recording evidence is incomplete; inspect the false checks above.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scene', choices=('start', 'status', 'logs', 'queries', 'inject', 'recover', 'verify', 'benchmark', 'check', 'stop'))
    parser.add_argument('--restarts', type=int, choices=(2, 3), default=3)
    args = parser.parse_args()
    if args.scene == 'start':
        if (RUNTIME / 'checkpoints').exists() or (RUNTIME / 'ratings.parquet').exists():
            raise RuntimeError('This recording directory already contains a replay. Use its running services or a fresh project copy; start never replays an existing dataset.')
        RUNTIME.mkdir(exist_ok=True)
        if not PROJECT_FILE.exists():
            project = 'movie-reviews-recording-' + uuid.uuid4().hex[:8]
            PROJECT_FILE.write_text(json.dumps(dict(project=project)))
            COMPOSE[3] = project
        compose('up', '--build', '-d')
        print('Recording dashboard: ' + URL + '/demo', flush=True)
    elif args.scene == 'status':
        print(json.dumps(status(), indent=2))
    elif args.scene == 'logs':
        compose('logs', '-f', '--tail', '30', 'batch', 'producer', 'consumer')
    elif args.scene == 'queries':
        compose('exec', '-T', 'serve', 'python3', '/app/queries.py')
    elif args.scene == 'inject':
        row = json.loads(input('Type the bad rating JSON: '))
        request = urllib.request.Request(URL + '/demo/inject', data=json.dumps(row).encode(),
                                         headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=30) as response:
            print(json.dumps(json.load(response), indent=2))
    elif args.scene == 'recover':
        recovery(args.restarts)
    elif args.scene == 'verify':
        verify()
    elif args.scene == 'benchmark':
        benchmark()
    elif args.scene == 'check':
        check()
    elif args.scene == 'stop':
        compose('stop')


if __name__ == '__main__':
    main()
