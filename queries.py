"""All answers are read from SQLite; elapsed time includes execute + fetchall."""
import argparse
import json
from time import perf_counter
from database import INDEXES, connect, indexes

SQL = {
1: """SELECT s.movie_id, m.title, s.avg_rating, s.rating_count
      FROM movie_stats s JOIN movies m USING(movie_id)
      WHERE s.rating_count >= 1000 ORDER BY s.avg_rating DESC, s.movie_id LIMIT 10""",
2: """SELECT year, genre, rating_count FROM genre_year g
      WHERE rating_count=(SELECT MAX(g2.rating_count) FROM genre_year g2 WHERE g2.year=g.year)
      ORDER BY year, genre""",
3: """SELECT h.hour_start, h.hour_end, h.movie_id, m.title, h.rating_count
      FROM hourly_counts h JOIN movies m USING(movie_id)
      WHERE h.hour_start=(SELECT MAX(hour_start) FROM hourly_counts)
      ORDER BY h.rating_count DESC,h.movie_id LIMIT 10""",
}


def answer(question, path=None, indexed=True):
    with connect(path) as db:
        if not db.execute("SELECT 1 FROM metadata WHERE key='batch_ready'").fetchone():
            raise RuntimeError("Batch results are not ready")
        started = perf_counter()
        rows = [dict(row) for row in db.execute(SQL[question]).fetchall()]
        elapsed = (perf_counter() - started) * 1000
        db.execute("INSERT INTO query_timings(question,indexed,elapsed_ms,result_rows) VALUES (?,?,?,?)", (question,indexed,elapsed,len(rows)))
    return {"question":question,"sql":SQL[question],"elapsed_ms":round(elapsed,4),"indexed":indexed,"status":"ready" if rows else "no_results_yet" if question==3 else "no_results", "rows":rows}


def benchmark(path=None):
    # Run offline with the consumer stopped: these statements change shared indexes.
    results = []
    try:
        with connect(path) as db:
            for name in INDEXES:
                db.execute(f"DROP INDEX IF EXISTS {name}")
        results.extend(answer(q,path,indexed=False) for q in SQL)
    finally:
        with connect(path) as db:
            indexes(db)
    results.extend(answer(q,path) for q in SQL)
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark",action="store_true")
    args = parser.parse_args()
    print(json.dumps(benchmark() if args.benchmark else [answer(q) for q in SQL], indent=2))
