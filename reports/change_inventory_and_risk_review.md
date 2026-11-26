# Complete change inventory and proposal risk review

Originally reviewed 23 November 2026 against baseline commit `5442e1c` (Initial project commit); updated 25 November with the five-minute grace change and its validation. This inventory includes prior work in the shared workspace. Git cannot attribute uncommitted changes to a particular person or chat. The original proposal PDF is preserved.

## Existing implementation changes

| File | Changes from the baseline |
|---|---|
| `.dockerignore` | Excludes Git, runtime data, checkpoints, reports, CSV/Parquet and Python caches from image builds; replaces older patterns. |
| `.gitignore` | Ignores database/runtime state, checkpoints, late/invalid records, old stream output, report logs and Python caches. |
| `Dockerfile` | Replaces Corretto/yum base with Python 3.11 Debian plus Java 17; pins Spark 3.5.8, Arrow 19.0.1 and kafka-python 2.2.15. |
| `docker-compose.yml` | Adds SQLite serving service and health check; shared database volume for batch/consumer/serve; Kafka persistence and initialization; configurable host port; laptop-oriented memory limits; batch-before-stream ordering; isolates the current `grace5` database/Kafka volumes and `stream_grace5` checkpoint from the earlier implementation. |
| `starter.py` | Uses two Spark threads, four shuffle partitions, 1 GiB driver setting; saves storage-size JSON; logs movie stage counts; adds deterministic Q1 tie ordering; loads complete batch aggregates into SQLite. |
| `quality.py` | Coalesces nullable range/ID predicates so missing rows are classified and quarantined rather than disappearing from both good and bad outputs. Existing four checks were already present in the baseline. |
| `producer.py` | Waits for the current consumer event-time watermark to pass the old probe windows before injecting five deliberate late rows, so the demo exercises real watermark classification instead of relying on a fixed delay. |
| `consumer.py` | Replaces the prior independent raw-quality, late-diagnostic and Spark-window queries with one offset-ordered `foreachBatch` classification/sink. It tracks the high event time, includes rows through `window_end + 300s`, quarantines strictly later valid rows, and commits hourly deltas, late rows, counts, batch marker and high-water time atomically to SQLite. |
| `compare_deep_dive.py` | Reads SQLite hourly counts instead of Parquet; aligns default years with the replay; exits with failure for mismatched or extra keys. Still defaults to only 100 source rows. |
| `README.md` | Rewrites startup, database/API query instructions, timing/index benchmark instructions, quality/quarantine behavior, recovery/reset rules, verification steps, source attribution and AI disclosure; explicitly describes one-million-row scope and latest event-time hour. |

## Added source files

- `database.py`: shared SQLite schema, WAL connections, atomic batch replacement, absolute keyed hourly upserts, committed-microbatch ledger, stage metrics, metadata, three ranking indexes and persisted query timings.
- `event_watermark.py`: common one-hour window boundary and strict five-minute grace calculation, shared by the stream consumer and its tests.
- `observability.py`: exposes cumulative raw/in-grace/late/quality counts, retained late samples and a conservation flag from the transactional ledger.
- `batch_database.py`: loads movie lookup, movie average/count and exploded genre/year aggregates from Parquet; can reload without regenerating Parquet.
- `queries.py`: all three database questions, deterministic ordering/ties, execute-plus-fetch timing, readiness/empty results and index comparison CLI.
- `serve.py`: standard-library HTTP service for health and all three questions, with JSON responses and error handling.
- `verify_results.py`: independent full-source CSV calculation of all three answers, total ratings and every hourly/movie key; saves validation JSON only after assertions pass. Assumes the default clean full replay; it is not a general oracle for accepted injected late events.
- `tests/test_serving.py`: five tests for thresholds/ties/latest hour, replay/absolute updates, transactional rollback, timings/benchmark and empty stream.
- `tests/test_watermark.py`: inclusive exact five-minute boundary, strictly-later quarantine, idempotent batch retry and transaction rollback/conservation tests.
- `tests/test_quality.py`: Spark fixture for four checks, quarantine and good+bad row conservation.
- `reports/build_report.py`: eight-page evidence-based PDF report generator, diagram, database alternative, proposal changes, risks/limits, scale discussion, contributions and acknowledgement.
- `reports/measure_report.py`: offline index benchmark, warmup plus 30 trials per question/state, medians/ranges and query plans.
- `reports/restart_experiment.py`: forced consumer SIGKILL/restart runner, progress snapshots and JSON evidence; preserves producer/database/checkpoints/Kafka. Requires a shared-services-free run. It does not itself establish a mid-window crash or full reconciliation.
- `reports/package_submission.py`: submission ZIP builder with CSV inputs, code, report and SHA-256 manifest; excludes mutable runtime state.

## Added documentation and generated artifacts

- `reports/proposal_tracking.md`: proposal-to-implementation decisions and evidence gaps.
- `reports/requirements_audit.md`: assignment cross-check, fixes, limits and submission items.
- `reports/verification.md`: batch/test evidence, failed bind-mounted database diagnosis and corrected volume-based replay status.
- `reports/report_status.md`: report evidence handoff and shared-service concurrency warning.
- `reports/watermark_validation.json`: current full-run counts, late-row samples, SQLite integrity result, query timings and independent CSV-oracle pass.
- `reports/storage_sizes.json`: measured CSV/Parquet sizes and batch row counts.
- `reports/restart_validation.json`: restart evidence structure; **two completed consumer SIGKILL/restarts recorded**.
- `reports/stream_progress.jsonl`: Spark query progress, watermark/state/input diagnostics.
- `reports/evidence/`: captured batch log, initial answers, local/container test logs, failed consumer log and failed restart-attempt log. Initial/failed artifacts do not prove the corrected final pipeline passed.
- `output/pdf/MovieLens_Final_Report.pdf`: generated report, rebuilt in this audit to reflect the reduced restart target; absent evidence remains explicitly flagged.
- `tmp/pdfs/`: report preview pages and Swift renderer; temporary QA material.
- `movies.parquet/` and `ratings.parquet/`: regenerated data output. Old tracked part files/checksum sidecars are deleted and new part files/sidecars appear; filenames and counts reflect a fresh run with different parallelism. The inventory below lists each file individually.
- Ignored runtime state (database files, checkpoints, Python caches and similar) may exist locally. These are not source changes or submission evidence by themselves and are not included in the Git-visible appendix.

## Risk 1: imperfect results after restarts or late arrivals

**Verdict: five-minute boundary classification and full source reconciliation demonstrated; forced restart for this new checkpoint generation remains unmeasured.**

The earlier implementation used a 15-minute Spark watermark plus a separate diagnostic query, so it could not prove which events were actually included or dropped at each window's end. The current `grace5` generation replaces that path with a single classifier and transactional sink. Kafka, database and checkpoints persist. The former macOS bind-mounted database failed; Compose now places SQLite in a Docker-managed volume with container-only access. The current run reconciled 1,000,005 raw stream rows: 1,000,000 source events included, five intentionally late events stored, zero quality failures; all 994,006 hourly keys match the independent CSV oracle.

Open items:

1. The current five-minute run and exact boundary unit test are in `reports/watermark_validation.json` and `tests/test_watermark.py`. A row exactly at `window_end + 300s` is included; strictly later rows are saved to `late_events` with original JSON, offsets, bounds and lateness.
2. Raw rows reconcile as incoming = in-grace + late + quality-failed. The run observed 1,000,005 = 1,000,000 + 5 + 0, with the late row records retained and SQLite `integrity_check` returning `ok`.
3. Earlier `restart_validation.json` evidence belongs to the prior watermark/checkpoint generation. Current-generation forced SIGKILL/restart behavior has not been measured; sink idempotence and transaction rollback are unit-tested, but those tests do not replace a process restart experiment.
4. Source duplicates at different Kafka offsets are not deduplicated across microbatches. Restarting the consumer and rerunning the producer are distinct operations; the ledger protects retrying a committed microbatch, not a fresh replay under new offsets.

The historical two-restart experiment should not be presented as a current-generation restart test. Remaining coverage: force a restart against the new grace5 volume/checkpoint and reconcile after recovery; global deduplication across fresh source replays is outside the current sink ledger contract.

## Risk 2: limited laptop memory

**Verdict: mitigations applied for the required one-million-row workload; peak-memory measurements remain optional additional evidence.**

Applied: one-million-row slice instead of 32M; Kafka 512 MiB heap/1 GiB container; consumer 1 GiB driver/3 GiB container; producer 1 GiB; serve 256 MiB; batch 2 GiB; two Spark threads; batch completes before streaming starts. Streaming service limits total about 5.25 GiB, excluding Docker/OS overhead. Limits are caps, not measured peak usage. Captured batch and stream evidence confirms successful processing of the required million rows. This satisfies the dataset-size requirement. Container caps are not measured peak RAM.

Processing 32M rows and staged five-year processing are outside the required scope; neither is unfinished work or a requirement gap. One million real ratings is sufficient. The producer materializes/sorts an entire year with Arrow; larger-scale memory concerns belong only in the optional scalability discussion. No saved peak-RAM measurement is available, but the required one-million-row batch/stream run completed.

## Changes made in the 23 November audit

1. Set restart harness default to 3, accepting only `--restarts 2` or `--restarts 3`; records requested target in new experiment JSON and prints actual count.
2. Updated README, proposal tracking, requirements audit and report generator to identify the professor-adjusted 2-3 target while preserving the original proposal PDF.
3. Corrected the tracking note: one initial Git commit exists; attribution of uncommitted edits remains unavailable.
4. Added this comprehensive inventory and risk assessment, including the complete Git-visible per-file appendix.
5. Rebuilt the report from its current evidence inputs; no runtime evidence was invented.

## Five-minute watermark follow-up changes (25 November)

| File | Change | Earlier problem addressed |
|---|---|---|
| `consumer.py` | One `foreachBatch` now classifies records in Kafka partition/offset order, computes per-batch conservation counts, and commits hourly deltas and late-row quarantine together. | Three separate queries observed different row sets/progress; the late diagnostic was not an exact record of what Spark dropped. |
| `event_watermark.py` | Centralizes UTC hour windows and an inclusive 300-second grace boundary. | No test proved how a row at the exact five-minute boundary should be treated. |
| `database.py` | Adds `late_events`, `stream_batches`, durable max-event-time state and transactional/idempotent batch commits. | Old late tracking lacked a reconciled durable row table; retry and count consistency were not one atomic sink operation. |
| `producer.py` | Waits until current-run watermark progress makes injected rows genuinely late. | Fixed sleeps could inject too early, before the consumer had advanced past the test windows. |
| `observability.py`, `demo.html`, `demo.py` | Show included, quarantined, failed and incoming totals, samples and conservation state; recording checkpoint follows grace5. | Previous monitoring could not report the requested exact in-time and set-aside row counts together. |
| `docker-compose.yml`, `.gitignore` | Isolates grace5 Kafka/database volumes and checkpoint; ignores the transient watermark marker. | Reusing old progress/state could mix the 15-minute run with the changed five-minute classifier. |
| `tests/test_watermark.py` | Covers exact boundary, later-than-grace quarantine, retry idempotence, transaction rollback and count mismatch. | Earlier tests did not exercise the grace boundary or transactional late-row conservation. |
| `README.md`, `reports/build_report.py`, `reports/report_status.md` | Document the rule, measured evidence and current restart limitation. | Earlier docs still described the old 15-minute/diagnostic behavior as current evidence. |
| `reports/capture_watermark_evidence.py`, `reports/watermark_validation.json` | Captures full replay counts, late samples, database integrity and oracle evidence. | No durable artifact showed incoming = included + late + quality failures for the new run. |
| `output/pdf/MovieLens_Final_Report.pdf`, `output/MovieLens_Final_Submission.zip` | Rebuilt the eight-page report and 74-file submission payload with the updated evidence and source. | Prior package/report did not reflect the five-minute implementation or run. |

## Complete Git-visible file appendix

Historical snapshot from the 23 November audit; M = modified, D = deleted, ?? = added/untracked. It predates the watermark follow-up table above; generated files were included individually in the original snapshot.

```text
 M .dockerignore
 M .gitignore
 M Dockerfile
 M README.md
 M compare_deep_dive.py
 M consumer.py
 M docker-compose.yml
 D movies.parquet/.part-00000-e9d1c45a-2d6f-44d1-8c93-188432302a89-c000.snappy.parquet.crc
 D movies.parquet/.part-00001-e9d1c45a-2d6f-44d1-8c93-188432302a89-c000.snappy.parquet.crc
 D movies.parquet/part-00000-e9d1c45a-2d6f-44d1-8c93-188432302a89-c000.snappy.parquet
 D movies.parquet/part-00001-e9d1c45a-2d6f-44d1-8c93-188432302a89-c000.snappy.parquet
 M producer.py
 M quality.py
 D ratings.parquet/year=1996/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1996/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1996/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1996/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1996/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1996/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1996/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1996/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1997/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1997/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1997/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1997/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1997/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1997/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1997/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1997/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1998/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1998/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1998/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1998/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1998/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1998/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1998/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1998/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1999/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1999/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1999/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1999/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=1999/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1999/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1999/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=1999/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2000/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2000/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2000/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2000/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2000/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2000/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2000/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2000/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2001/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2001/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2001/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2001/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2001/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2001/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2001/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2001/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2002/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2002/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2002/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2002/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2002/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2002/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2002/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2002/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2003/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2003/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2003/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2003/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2003/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2003/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2003/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2003/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2004/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2004/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2004/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2004/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2004/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2004/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2004/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2004/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2005/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2005/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2005/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2005/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2005/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2005/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2005/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2005/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2006/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2006/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2006/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2006/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2006/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2006/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2006/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2006/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2007/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2007/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2007/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2007/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2007/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2007/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2007/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2007/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2008/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2008/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2008/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2008/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2008/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2008/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2008/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2008/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2009/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2009/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2009/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2009/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2009/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2009/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2009/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2009/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2010/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2010/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2010/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2010/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2010/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2010/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2010/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2010/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2011/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2011/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2011/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2011/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2011/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2011/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2011/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2011/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2012/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2012/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2012/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2012/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2012/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2012/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2012/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2012/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2013/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2013/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2013/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2013/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2013/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2013/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2013/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2013/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2014/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2014/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2014/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2014/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2014/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2014/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2014/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2014/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2015/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2015/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2015/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2015/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2015/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2015/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2015/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2015/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2016/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2016/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2016/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2016/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2016/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2016/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2016/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2016/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2017/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2017/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2017/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2017/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2017/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2017/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2017/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2017/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2018/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2018/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2018/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2018/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2018/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2018/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2018/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2018/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2019/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2019/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2019/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2019/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2019/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2019/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2019/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2019/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2020/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2020/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2020/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2020/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2020/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2020/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2020/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2020/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2021/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2021/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2021/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2021/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2021/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2021/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2021/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2021/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2022/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2022/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2022/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2022/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2022/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2022/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2022/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2022/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2023/.part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2023/.part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2023/.part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2023/.part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet.crc
 D ratings.parquet/year=2023/part-00000-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2023/part-00001-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2023/part-00002-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 D ratings.parquet/year=2023/part-00003-684f4165-4735-4be7-babe-d4aa17c48b66.c000.snappy.parquet
 M starter.py
?? batch_database.py
?? database.py
?? movies.parquet/.part-00000-b8375248-29d6-47dc-a061-80b06e65d0fc-c000.snappy.parquet.crc
?? movies.parquet/part-00000-b8375248-29d6-47dc-a061-80b06e65d0fc-c000.snappy.parquet
?? output/pdf/MovieLens_Final_Report.pdf
?? queries.py
?? ratings.parquet/year=1996/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1996/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1996/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=1996/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=1997/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1997/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1997/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=1997/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=1998/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1998/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1998/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=1998/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=1999/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1999/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=1999/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=1999/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2000/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2000/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2000/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2000/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2001/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2001/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2001/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2001/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2002/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2002/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2002/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2002/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2003/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2003/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2003/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2003/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2004/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2004/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2004/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2004/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2005/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2005/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2005/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2005/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2006/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2006/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2006/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2006/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2007/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2007/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2007/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2007/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2008/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2008/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2008/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2008/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2009/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2009/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2009/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2009/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2010/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2010/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2010/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2010/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2011/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2011/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2011/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2011/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2012/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2012/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2012/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2012/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2013/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2013/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2013/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2013/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2014/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2014/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2014/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2014/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2015/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2015/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2015/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2015/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2016/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2016/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2016/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2016/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2017/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2017/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2017/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2017/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2018/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2018/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2018/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2018/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2019/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2019/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2019/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2019/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2020/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2020/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2020/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2020/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2021/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2021/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2021/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2021/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2022/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2022/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2022/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2022/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2023/.part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2023/.part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet.crc
?? ratings.parquet/year=2023/part-00000-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? ratings.parquet/year=2023/part-00001-340fd869-28f1-48d3-909c-f8681f67bd0c.c000.snappy.parquet
?? reports/build_report.py
?? reports/change_inventory_and_risk_review.md
?? reports/compose.evidence.yml
?? reports/evidence/answers_initial.json
?? reports/evidence/batch.log
?? reports/evidence/consumer_initial_failure.log
?? reports/evidence/container_tests.txt
?? reports/evidence/local_tests.txt
?? reports/evidence/restart_experiment.log
?? reports/measure_report.py
?? reports/package_submission.py
?? reports/proposal_tracking.md
?? reports/report_status.md
?? reports/requirements_audit.md
?? reports/restart_experiment.py
?? reports/restart_validation.json
?? reports/storage_sizes.json
?? reports/stream_progress.jsonl
?? reports/verification.md
?? serve.py
?? tests/test_quality.py
?? tests/test_serving.py
?? tmp/pdfs/page-1.png
?? tmp/pdfs/page-2.png
?? tmp/pdfs/page-3.png
?? tmp/pdfs/page-4.png
?? tmp/pdfs/page-5.png
?? tmp/pdfs/page-6.png
?? tmp/pdfs/page-7.png
?? tmp/pdfs/page-8.png
?? tmp/pdfs/render.swift
?? verify_results.py
```


## Historical final report-session validation (previous checkpoint generation)

Two consumer SIGKILL/restarts completed after source replay while the latest event-time window remained open. Each restart consumed one labelled old recovery probe; source rating totals stayed 1,000,000 before and after. Spark observed 1,000,007 total events: 1,000,000 source, five default late events and two recovery probes. All seven test events were dropped by the watermark, with two open-window state keys retained. The full CSV oracle passed again after both restarts: all three answers and all 994,006 hourly keys match. See restart_validation.json, recovery_probes.json, stream_summary.json and evidence/post_restart_csv_verification.log.

The PDF uses final_index_benchmark.json for the frozen 30-trial measurements; earlier timing artifacts are separate runs. The report is eight pages. Confirmed contributions: Nafisa Serve/checkpoint video; Izma Store/Batch; Homa Stream and <=3-command startup; Nitant Quality/Monitoring.

This historical evidence uses the previous watermark/checkpoint generation and must not be conflated with the grace5 run. Current five-minute evidence is in `watermark_validation.json`; no forced restart has yet been run against grace5. Global source-event deduplication across fresh Kafka offsets remains unimplemented.

## Follow-up recheck

The current restart evidence supersedes the earlier zero-restart finding: two consumer SIGKILL/restarts are recorded. One million source ratings meets the data requirement; 32M processing is outside required scope. Restarted queries retained two state keys and processed recovery probes at offsets 1,000,005 and 1,000,006, each rejected by the watermark with source aggregates unchanged. See restart_validation.json and stream_summary.json.
