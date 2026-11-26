"""Independent clean-input CSV oracle for the default full replay (not sampled runs)."""
import csv
import json
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from database import connect
from queries import answer

movies = {}
with open('movies.csv', newline='', encoding='utf-8') as source:
    for row in csv.DictReader(source):
        movies[int(row['movieId'])] = row
stats = defaultdict(lambda: [0.0, 0])
genres = Counter()
latest_hour = -1
latest_counts = Counter()
oracle_dir = tempfile.TemporaryDirectory(prefix='movielens-oracle-')
oracle_path = oracle_dir.name + '/hourly.sqlite3'
oracle = sqlite3.connect(oracle_path)
oracle.execute('CREATE TABLE expected(hour_start TEXT,movie_id INTEGER,rating_count INTEGER,PRIMARY KEY(hour_start,movie_id))')
oracle.execute('BEGIN')
rows = 0
with open('ratings.csv', newline='', encoding='utf-8') as source:
    for row in csv.DictReader(source):
        movie_id, rating, timestamp = int(row['movieId']), float(row['rating']), int(row['timestamp'])
        rows += 1
        stats[movie_id][0] += rating
        stats[movie_id][1] += 1
        year = datetime.fromtimestamp(timestamp, timezone.utc).year
        for genre in movies[movie_id]['genres'].split('|'):
            genres[year,genre] += 1
        hour = timestamp // 3600 * 3600
        oracle.execute('INSERT INTO expected VALUES (?,?,1) ON CONFLICT(hour_start,movie_id) DO UPDATE SET rating_count=rating_count+1', (datetime.fromtimestamp(hour, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),movie_id))
        if hour > latest_hour:
            latest_hour, latest_counts = hour, Counter()
        if hour == latest_hour:
            latest_counts[movie_id] += 1
oracle.commit()
hourly_key_count = oracle.execute('SELECT COUNT(*) FROM expected').fetchone()[0]
oracle.close()
expected1 = sorted([(mid,total/count,count) for mid,(total,count) in stats.items() if count>=1000],key=lambda r:(-r[1],r[0]))[:10]
year_max = defaultdict(int)
for (year,genre),count in genres.items():
    year_max[year] = max(year_max[year],count)
expected2 = sorted((year,genre,count) for (year,genre),count in genres.items() if count==year_max[year])
expected3 = sorted(latest_counts.items(),key=lambda r:(-r[1],r[0]))[:10]
answers = [answer(q) for q in (1,2,3)]
actual1 = [(r['movie_id'],r['avg_rating'],r['rating_count']) for r in answers[0]['rows']]
actual2 = [(r['year'],r['genre'],r['rating_count']) for r in answers[1]['rows']]
actual3 = [(r['movie_id'],r['rating_count']) for r in answers[2]['rows']]
assert actual1==expected1, 'Q1 differs from CSV oracle'
assert actual2==expected2, 'Q2 differs from CSV oracle'
assert actual3==expected3, 'Q3 differs from CSV oracle (consumer may still be catching up)'
expected_hour = datetime.fromtimestamp(latest_hour, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
assert answers[2]['rows'] and all(r['hour_start']==expected_hour for r in answers[2]['rows']), 'Q3 uses the wrong event-time hour'
with connect() as db:
    stream_total = db.execute('SELECT SUM(rating_count) FROM hourly_counts').fetchone()[0]
assert stream_total==rows, f'Stream total {stream_total} != CSV total {rows}'
with connect() as db:
    db.execute('ATTACH DATABASE ? AS oracle', (oracle_path,))
    missing = db.execute('SELECT hour_start,movie_id,rating_count FROM oracle.expected EXCEPT SELECT hour_start,movie_id,rating_count FROM hourly_counts LIMIT 1').fetchone()
    extra = db.execute('SELECT hour_start,movie_id,rating_count FROM hourly_counts EXCEPT SELECT hour_start,movie_id,rating_count FROM oracle.expected LIMIT 1').fetchone()
    assert missing is None and extra is None, 'Full hourly key/count comparison differs from CSV'
oracle_dir.cleanup()

report = {'csv_ratings':rows,'stream_rating_total':stream_total,'checks':{'q1':True,'q2':True,'q3':True,'stream_total':True,'all_hourly_keys':True},'hourly_keys_compared':hourly_key_count,'answers':answers}
with open('reports/validation.json','w') as handle:
    json.dump(report,handle,indent=2)
print(json.dumps(report,indent=2))
