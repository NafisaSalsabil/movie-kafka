"""Benchmark an isolated snapshot or an offline database; never a live writer database."""
import sys, os, json, statistics, platform, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import connect,indexes,INDEXES
from queries import answer
with connect() as db:
    cardinalities={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ('movies','movie_stats','genre_year','hourly_counts')}
    source_total=db.execute('SELECT SUM(rating_count) FROM hourly_counts').fetchone()[0]
results={}
for state in (False,True):
    with connect() as db:
        if state: indexes(db)
        else:
            for name in INDEXES: db.execute('DROP INDEX IF EXISTS '+name)
    results[str(state)]={}
    for q in (1,2,3):
        answer(q,indexed=state)
        samples=[answer(q,indexed=state)['elapsed_ms'] for _ in range(30)]
        results[str(state)][str(q)]={'median_ms':statistics.median(samples),'min_ms':min(samples),'max_ms':max(samples),'n':len(samples)}
with connect() as db:
    plans={str(q):[tuple(r) for r in db.execute('EXPLAIN QUERY PLAN '+__import__('queries').SQL[q])] for q in (1,2,3)}
json.dump({'method':'Isolated SQLite backup snapshot; one warmup then 30 execute+fetch trials per question, unindexed then indexed; primary keys retained. Sequential order and OS cache are limitations.','cardinalities':cardinalities,'snapshot_stream_ratings':source_total,'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,'results':results,'indexed_plans':plans},open('reports/report_index_benchmark.json','w'),indent=2)
print(json.dumps(results,indent=2))
