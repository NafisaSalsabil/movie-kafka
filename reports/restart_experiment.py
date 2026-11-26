"""Host-side forced-crash evidence runner. Never resets state or replays producer."""
import argparse,subprocess,json,time
from pathlib import Path

def state():
    try:
        code="import json; from database import connect; context=connect(); d=context.__enter__(); print(json.dumps([d.execute('select coalesce(sum(rating_count),0) from hourly_counts').fetchone()[0],d.execute(\"select count(*) from sink_batches where stream='hourly'\").fetchone()[0]]))"
        total,batches=json.loads(subprocess.check_output(['docker','compose','exec','-T','serve','python3','-c',code],text=True))
    except (ValueError,subprocess.CalledProcessError): total=batches=0
    progress=[]
    f=Path('reports/stream_progress.jsonl')
    if f.exists():
        for line in f.read_text().splitlines():
            try:
                j=json.loads(line)
                if j.get('name')=='hourly':progress.append(j)
            except ValueError:pass
    return {'rating_total':total,'committed_batches':batches,'progress_events':len(progress),'last_progress':progress[-1] if progress else None}

def wait_until(fn,seconds=360):
    start=time.time()
    while time.time()-start<seconds:
        s=state()
        if fn(s):return s
        time.sleep(2)
    raise RuntimeError('Timed out waiting for stream progress')

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--restarts',type=int,choices=(2,3),default=3,help='Professor-adjusted target (default: 3)')
args=parser.parse_args()

results={'target_restarts':args.restarts,'method':'SIGKILL consumer only, retain database/checkpoints/Kafka. Restarts preserve the latest open event-time window; producer activity is recorded. One labelled old probe triggers progress after each restart. Restart and require a new hourly progress event each time. Final full CSV oracle required.','restarts':[]}
Path('reports/restart_validation.json').write_text(json.dumps(results,indent=2))
for i in range(args.restarts):
    before=wait_until(lambda s:s['rating_total']>0)
    running=subprocess.check_output(['docker','compose','ps','--status','running','--services'],text=True).splitlines()
    entry={'restart':i+1,'before':before,'producer_running': 'producer' in running,'signal':'SIGKILL'}
    subprocess.run(['docker','compose','kill','-s','SIGKILL','consumer'],check=True)
    subprocess.run(['docker','start','movie-reviews-consumer-1'],check=True)
    subprocess.run(['docker','compose','exec','-T','serve','python3','reports/send_recovery_probe.py'],check=True)
    after=wait_until(lambda s:s['progress_events']>before['progress_events'],seconds=360)
    entry['after']=after
    results['restarts'].append(entry)
    Path('reports/restart_validation.json').write_text(json.dumps(results,indent=2))
    print('Completed restart',i+1,'total',after['rating_total'],flush=True)
print(f'{args.restarts} restarts complete; run CSV oracle after catchup.',flush=True)
