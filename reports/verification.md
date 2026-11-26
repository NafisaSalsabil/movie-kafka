# Serving implementation validation

Status: full clean-replay validation PASSED after correcting two runtime integration defects.

- Batch conversion completed: 1,000,000 ratings in/out, 87,585 movies. All four rating quality checks reported zero failures. See storage_sizes.json and captured batch logs.
- All six tests passed in Docker, including the Spark bad-row/conservation fixture. Existing captured container_tests.txt records this run.
- First stream attempt failed with a malformed SQLite database on a macOS bind mount. Do not use data/analytics.sqlite3 for current validation.
- Corrected database: /database/analytics.sqlite3 **inside Docker volume movie-reviews_analytics-data**. Batch, consumer and serve share it. Use `docker compose exec -T serve python3 ...` for all live queries, CSV validation, and benchmark scripts; do not run them on the macOS host.
- Batch aggregates have been reloaded into that new database from existing Parquet with `docker compose run --rm --no-deps batch python3 batch_database.py`.
- The generated Kafka ratings topic was reset and replay restarted with new checkpoints/stream_sqlite state after the failed run. Old checkpoints and failed database remain as diagnostic evidence, not active state.
- Kafka persistence uses /var/lib/kafka/data, the image-owned writable directory. The evidence run retains its separate host port 19092 override; standard README startup uses 9092.
- Producer late injection now waits for an hourly Spark progress watermark observed after the current replay began, avoiding the fixed-20-second startup race.

Final CSV reconciliation passed in the batch container: all three query answers, the total 1,000,000 ratings, and every one of the 994,006 movie-hour key/count rows match the source CSVs. The latest stored hour is 2023-10-12 22:00:00–23:00:00 UTC. See validation.json for complete answers and timings (Q1 4.4667 ms, Q2 3.9347 ms, Q3 2.2617 ms in that run).

Spark progress confirms offset 1,000,005 was consumed: 1,000,000 source records plus five late test records. All five test records were dropped by the hourly watermark, leaving the source total unchanged. See stream_progress.jsonl, hourly batch 3, watermark 2023-10-12T22:19:15Z, numRowsDroppedByWatermark=5.

The full CSV oracle belongs in the 2 GiB batch container. Attempting it concurrently with benchmarks inside the 256 MiB API container exceeded that validation process's memory budget; the API itself remained healthy. The final successful check used `docker compose run --rm --no-deps batch python3 verify_results.py`.

Previously captured failed-restart artifacts belong to the failed bind-mounted database run and cannot be treated as a verdict on the corrected volume-based sink. Newer evidence in restart_validation.json records two completed SIGKILL/restarts, meeting the professor-adjusted count. Both followed source replay while the latest event-time window retained state. Each resumed reading an old probe, dropped by the watermark; the stored source total remained 1,000,000. This is not evidence of an active-write crash.

Before/after timing comparisons must be isolated. The earlier serve_index_benchmark.json attempt overlapped another benchmark and is explicitly marked invalid for comparison. A subsequent isolated 30-trial run is captured in index_benchmark.json, with the consumer stopped and no other database jobs active.

## Completed serving benchmark

Isolated 30-trial warm medians before/after secondary indexes: Q1 5.8383 / 0.18885 ms; Q2 9.2524 / 0.7011 ms; Q3 0.30125 / 0.24325 ms. Primary keys remain in both states. Indexes restored afterward. Live HTTP checks subsequently returned ready for all three questions; integrity_check was ok and the total remained 1,000,000. See serving_http.json. The consumer was restored independently with `docker compose up -d --no-deps consumer` to avoid rerunning completed batch/producer services.


## Final report-session validation (supersedes earlier pending evidence)

Two consumer SIGKILL/restarts completed after source replay while the latest event-time window remained open. Each restart consumed one labelled old recovery probe; source rating totals stayed 1,000,000 before and after. Spark observed 1,000,007 total events: 1,000,000 source, five default late events and two recovery probes. All seven test events were dropped by the watermark, with two open-window state keys retained. The full CSV oracle passed again after both restarts: all three answers and all 994,006 hourly keys match. See restart_validation.json, recovery_probes.json, stream_summary.json and evidence/post_restart_csv_verification.log.

The PDF uses final_index_benchmark.json for the frozen 30-trial measurements; earlier timing artifacts are separate runs. The report is eight pages. Confirmed contributions: Nafisa Serve/checkpoint video; Izma Store/Batch; Homa Stream and <=3-command startup; Nitant Quality/Monitoring.

Remaining coverage limits: no dedicated within-watermark boundary fixture; no global source-event deduplication across fresh Kafka offsets. The two forced tests preserve open-window state after replay; interrupted in-flight sink writes are tested separately by the rollback unit test. These limits are stated in the report rather than represented as completed experiments.
