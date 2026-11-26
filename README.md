# MovieLens: store, stream, and serve

**Final screen recording:** [RECORDING.md](RECORDING.md) provides the tested demo commands and a 7:30 speaking plan for all four members. Run `python3 demo.py start` in a fresh copy for an isolated recording project, then open [the live recording dashboard](http://localhost:8001/demo). It shows real database arrivals, all three SQL answers, hand-entered bad-row rejection/counts, and measured deep-part recovery. The normal service also serves this view at [localhost:8000/demo](http://localhost:8000/demo). Recording mode preserves the normal project's state; its three forced restarts target only the recording consumer.

Run on one laptop with Docker Desktop running and at least 8 GB RAM. Allocate about 6 GB or more to Docker; validation used a 7.75 GiB Docker memory budget. First build requires internet for images, Python packages, and the Spark Kafka connector. Ports 8000 and 9092 must be free. If 9092 is occupied, use `KAFKA_HOST_PORT=19092 docker compose up --build -d` as the third startup command. A one-shot Kafka init service sets ownership of the named log volume before the non-root broker starts.

## Start from a fresh copy in three commands

```sh
git clone https://github.com/NafisaSalsabil/movie-kafka.git
cd movie-kafka
docker compose up --build -d
```

These instructions require the completed files in this working copy to be committed to that repository before others clone it. No local Python or Java installation is required. Allow batch conversion to finish before querying; consumer and serving start automatically afterward. The repository supplies **1,000,000 ratings and 87,585 movies**, satisfying the dataset-size requirement. Processing all 32M ratings is not required.

## Part 1: store and batch

`starter.py` checks ratings, writes ratings to `ratings.parquet/year=.../`, writes the movie lookup to `movies.parquet/` (movies have no rating year), compares CSV and Parquet sizes, and executes both analytical questions in Spark from Parquet. Sizes and row counts are saved to `reports/storage_sizes.json`; full logs are available via `docker compose logs batch`.

Batch aggregates and movie titles load transactionally into **SQLite** at `/database/analytics.sqlite3` inside the `analytics-grace5` Docker volume. Do not open an active SQLite database across macOS/Linux bind mounts; access it through the containers. The database contains `movies`, `movie_stats`, `genre_year`, and `hourly_counts`, plus `late_events`, `stream_batches`, timing, sink replay, and stage metric tables. SQLite needs no separate database container, fits the single-laptop requirement, and uses WAL and a 60-second lock timeout for concurrent readers and the single streaming writer. The `analytics-grace5` and `kafka-grace5` volumes isolate this watermark/checkpoint version from data created by the earlier 15-minute implementation.

To reload batch aggregates from existing Parquet without repeating conversion, run `docker compose run --rm --no-deps batch python3 batch_database.py`.

## Part 2: stream

The producer reads year partitions, sorts by timestamp, and sends one Kafka partition as fast as possible (accelerated replay, with no simulated event clock). Defaults select 1996–2023, covering the bundled rows. Five intentionally altered, older events are injected only after the consumer reports that the watermark has passed their windows; these are test events, not source CSV rows. The Spark consumer tracks UTC one-hour windows with a five-minute event-time watermark. Each microbatch reads at most 100,000 offsets by default (`MAX_OFFSETS_PER_TRIGGER`) to bound the input workload.

The watermark is the greatest valid event timestamp observed so far minus five minutes. Each Kafka row is classified in partition/offset arrival order using the watermark that existed just before it arrived. A row is included through the exact boundary `window_end + 5 minutes`; it is late only when the prior high-water mark is strictly later than that boundary. Included rows update the stored hourly count. Later rows are retained in SQLite's `late_events` table with their source partition/offset, event timestamp, window bounds, watermark, and lateness. The SQLite `stream_batches` ledger records incoming, valid, quality-failed, included, and late row totals in the same transaction as the hourly update and late-row quarantine.

For every microbatch, valid incoming rows reconcile exactly as `included within five minutes + quarantined late rows`. Raw incoming rows also include rows rejected by the four Part 4 data checks, so the full conservation equation is `raw incoming = included + late + quality failures`. The live status endpoint and recording dashboard show all three categories and a conservation pass/fail flag. `checkpoints/stream_grace5/` is a separate checkpoint generation for this changed sink behavior; the event-time high-water mark lives transactionally in SQLite.

Hourly count **deltas** are added to SQLite by `(hour_start, movie_id)` so an in-grace arrival can revise a previously stored window. Each microbatch, late-row quarantine, maximum event time, quality counts, and replay marker commit in one transaction. A crash before commit rolls them back; retry after commit skips the already committed batch. Keep the database, Kafka volume, and Spark checkpoints together; deleting only one breaks the replay contract. The database retains prior hours after their five-minute grace expires.

Restart only the consumer (do not restart the producer for this test):

```sh
docker compose restart consumer
```

The professor-adjusted recovery target is **2-3 forced consumer restarts**, including one during active processing of an hourly window. The original proposal specified five. In an isolated run, use `python3 reports/restart_experiment.py --restarts 3` (or `--restarts 2`), then run `docker compose run --rm --no-deps batch python3 verify_results.py` after catch-up. Do not run the harness while another person or chat is operating these services. The harness records process recovery; inspect offsets/window progress to establish the mid-window case. An empty `restarts` list is no evidence of successful recovery. Five injected late test events are separate from the restart target.

The producer is a finite replay task. Starting it again replays the source; sink idempotence protects retrying a Spark microbatch, not new source messages with different Kafka offsets. Duplicate validation is performed in batch and monitored within each stream microbatch; cross-microbatch source-event deduplication is not implemented.

## Part 3: serve

Open any endpoint in a browser or use curl:

- [Question 1](http://localhost:8000/questions/1): ten highest-rated movies with at least 1,000 ratings. Equal averages break by movie ID.
- [Question 2](http://localhost:8000/questions/2): most-rated genre per rating year. Each genre of a multi-genre movie receives a count; tied winners are returned.
- [Question 3](http://localhost:8000/questions/3): top ten movies in the latest stored **event-time hour**, with window boundaries in the response. This follows the proposal's tumbling-window definition; it is not the wall-clock hour today or a sliding 60-minute query. The latest window may still receive updates.

Every JSON answer contains `elapsed_ms`, measured with `perf_counter` around SQL execution **and fetching all result rows**. This excludes connection setup, JSON serialization, and network time. Every measurement is also saved in `query_timings`. An empty stream returns `no_results_yet` until the first hourly microbatch commits.

Print all answers and timings:

```sh
docker compose exec serve python3 queries.py
```

Indexes cover movie average ranking (partial index for the 1,000-rating threshold), genre/year ranking, and latest-hour count ranking. To measure before/after these secondary indexes, stop the consumer first so it cannot recreate indexes during the benchmark:

```sh
docker compose stop consumer
docker compose exec serve python3 queries.py --benchmark
docker compose up -d --no-deps consumer
```

Primary keys remain in both measurements. This is one measured run per question per index state, with a warm local database; results are illustrative, not a statistically controlled performance claim. Avoid concurrent API requests during the benchmark. Indexes are restored even if a query fails.

## Part 4: quality and monitoring

`quality.py` prints failed row counts for four checks: missing required fields, rating range 0.5–5.0, positive IDs/timestamp, and duplicate `(userId,movieId,timestamp)` keys. Batch failures are quarantined to `bad_ratings/`; stream quality failures to `bad_ratings_stream/`; producer failures to `bad_ratings_producer.jsonl`. Stream rows beyond the five-minute event-time grace are stored in the separate SQLite `late_events` table. An alert prints when more than 1% fail.

Stage logs report input, output, and failure counts for rating validation, Parquet writes, replay, and database loads. Database aggregate counts count **aggregate rows**, not source ratings. The audit in `reports/requirements_audit.md` distinguishes implemented minimums from remaining deep-dive evidence and monitoring gaps.

## Verify and stop

Regression tests, including a small Spark quality fixture:

```sh
docker compose run --rm --no-deps batch python3 -m unittest discover -s tests -v
```

For an exactly matching stream/batch comparison, use an isolated fresh run and the same producer year bounds and message limit. `compare_deep_dive.py` defaults to the earliest 100 rows from 1996–2023; it must not be used against the full default replay as if that were an equal input. It exits nonzero for mismatches or extra stream keys. Stop the producer before the consumer only after its replay completes, and let the consumer catch up before comparison.

```sh
docker compose run --rm batch python3 compare_deep_dive.py --start-year 1996 --end-year 2023 --max-messages 100
```

For the complete default replay, independently check all answers and the total against the clean CSV inputs in the batch container (the API container has a smaller memory limit):

```sh
docker compose run --rm --no-deps batch python3 verify_results.py
```

Stop services without deleting persisted state:

```sh
docker compose down
```

A clean re-run requires clearing **all** generated database, checkpoint, late-state, and Kafka data together. Back up results first. Do not remove just checkpoints while retaining `sink_batches` in SQLite.

## Data attribution and coursework disclosure

Data: [MovieLens 32M, GroupLens Research](https://grouplens.org/datasets/movielens/32m/). This repository uses a one-million-rating slice plus its movie lookup. Data and transformations remain subject to the [MovieLens usage conditions](https://files.grouplens.org/datasets/movielens/ml-32m-README.html), including research use, acknowledgement, redistribution under the same conditions, and permission for commercial use.

Dataset citation: F. Maxwell Harper and Joseph A. Konstan (2015), *The MovieLens Datasets: History and Context*, ACM TiiS, [doi:10.1145/2827872](https://doi.org/10.1145/2827872).

AI acknowledgement for this implementation: OpenAI Codex helped implement the SQLite serving layer, timed queries, regression tests, Docker integration fixes, and the requirements audit. The team must review the code, confirm individual contributions, and disclose any other AI assistance in its final report.
