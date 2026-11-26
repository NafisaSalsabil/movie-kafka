# Requirements audit

Scope: the supplied master branch from NafisaSalsabil/movie-kafka plus the serving implementation in this workspace. Runtime evidence is recorded below after validation. The bundled ratings file has 1,000,000 data rows; the movie lookup has 87,585.

| Requirement | Implementation / status |
|---|---|
| Run on one laptop with Docker Desktop | Compose includes Kafka, Spark batch, Spark consumer, finite producer, and HTTP service. SQLite runs inside the Python drivers. Runtime validation below. |
| At least 8 GB memory | Kafka 1 GiB, consumer 3 GiB, producer 1 GiB, serve 256 MiB; batch 2 GiB finishes before consumer/producer/serve start. Maximum configured concurrent limits are 5.25 GiB in streaming. Docker itself and host OS need additional memory. |
| Fresh copy startup in <=3 commands | README gives clone, cd, compose up. Local changes must first be committed/published to make the remote clone contain them. |
| Raw CSV converted to chosen format | starter.py rewrites Parquet from CSV. Ratings partition by UTC rating year. Movies are a dimension without a rating year. |
| File sizes before and after | Printed in batch logs and saved as exact bytes in reports/storage_sizes.json. |
| Spark answers at least Q1 and Q2 from stored data | Both existing Spark SQL queries read the freshly written Parquet; complete movie and genre aggregates also load into SQLite. |
| Event-time-ordered accelerated Kafka replay | Producer sorts each year by timestamp and sends one partition without event-time sleep. Five intentionally out-of-order late test events follow the main replay. |
| Consumer keeps running result | Spark microbatches maintain UTC 1h tumbling counts. The event-time watermark gives each window a five-minute grace; through-boundary rows increment SQLite and later valid rows are stored in `late_events`. |
| Batch and stream in one database | `/database/analytics.sqlite3` in the `analytics-grace5` Docker volume contains movie titles, movie averages/counts, genre/year counts, hourly stream counts, and late event rows. |
| Database answers all three with response times | queries.py and HTTP /questions/1, /2, /3. perf_counter measures execute + fetchall; query_timings stores each measurement. |
| Four automatic checks, with failed counts | Missing, rating range, positive IDs/timestamp, duplicate keys. Wired into batch and the unified streaming microbatch processor; failures print per check. |
| Rows in/out counted at every stage | Ratings CSV validation, Parquet, movies Parquet, producer replay, stream microbatches, and database loads report counts. `stream_batches` transactionally records raw input, quality failures, within-grace rows, and late rows; conservation is checked and exposed by `/demo/status`. Injected late rows remain a separate producer test stage. |
| Going deep: formats/layouts, size AND runtime comparison | NOT implemented: CSV-vs-Parquet size comparison exists, but no controlled read-runtime comparison or second partition layout. Optional example; stream was the chosen deep dive. |
| Going deep: index design, before/after timings | Three secondary ranking indexes; queries.py --benchmark drops/recreates them around timed queries. Primary keys remain. Stop consumer during benchmark. |
| Going deep: late event correctness | Event-time rows are classified in Kafka partition/offset order against a persisted high-water mark. The exact `window_end + 5 minutes` boundary is included; a row is late only after it. Late rows and counters commit atomically with hourly counts. Unit tests cover the boundary, quarantine, replay idempotence, and conservation; Docker end-to-end replay evidence for this implementation remains to be captured. |
| Going deep: safe consumer restart | Spark offsets/state checkpointed; SQLite counts and batch ledger commit atomically. Kafka logs and the Linux-only SQLite volume now persist too. Unit tests cover sink replay and interrupted transaction rollback. 2-3 forced consumer restarts (professor-adjusted) including mid-window require explicit runtime evidence; not assumed from code. |
| Going deep: bad rows in their own database table | Rows rejected by the four quality checks still go to Parquet/JSONL. Event-time-late rows are now durably stored in the SQLite `late_events` table with their Kafka position and watermark details. |
| Going deep: quality threshold alert | Existing >1% alert is wired into batch, producer validation, and stream quality monitor. It prints to logs, not an external notification service. |

## Corrections made while completing serve

- Added transactional SQLite sinks and API/CLI queries with persisted response times.
- Added indexes and an offline before/after benchmark.
- Replaced the per-hour/per-movie Parquet streaming sink with keyed SQLite upserts to avoid thousands of tiny partitions.
- Corrected SQL null propagation in quality classification: a missing field must be quarantined rather than fall out of both good and bad DataFrames.
- Separated stream and batch quarantine paths to avoid mixed schemas.
- Reduced memory limits, limited Spark to two local threads, sequenced services after batch, and persisted Kafka logs.
- Saved machine-readable storage sizes, Spark progress, and added independent CSV-result validation.
- Updated the old stream comparison to read SQLite, aligned year defaults with producer, and made mismatches return a failing exit code.

## Remaining limits to explain in the demonstration

- The required dataset size is met by 1,000,000 real ratings. Processing the original 32M dataset is not required.
- Four checks validate ratings; movie schema/foreign-key validity is not comprehensively checked. Schema is inferred for batch CSVs.
- Stream duplicate monitoring is microbatch-local and independent of aggregation. Source duplicates in separate Kafka offsets are not deduplicated across batches. Sink idempotence handles checkpoint retry, not arbitrary producer replay.
- Producer send futures are not individually checked; broker failures may require stronger acknowledgement accounting for the deep-dive no-loss claim.
- Quality-rejected rows remain in file-based quarantine; event-time-late rows are in SQLite `late_events`. Source duplicates across fresh Kafka offsets are not deduplicated.
- Keep database, checkpoints, late-state and Kafka volume together. Resetting only one invalidates the recovery contract.
- The old 100-row comparison is only meaningful when producer input uses that same slice; default full replay requires the full-CSV oracle in verify_results.py instead.
- No second storage-layout benchmark, full quarantine database table, or external alerting was added; these are optional extensions.

## Runtime evidence

See reports/storage_sizes.json, reports/validation.json, reports/index_benchmark.json and reports/verification.md when present. Missing artifacts indicate that corresponding check has not been completed; implementation alone is not proof of runtime success.

## Data and submission requirements outside the four pipeline parts

The full assignment PDF also requires the following; these are not all verifiable from running code:

| Requirement | Cross-check |
|---|---|
| Real public dataset, no synthetic replacement | MovieLens source and usage conditions are linked in README; the one-million-rating source slice is real data. Five injected older rows are explicitly test traffic. |
| >=1,000,000 rows through Part 1 | Runtime batch logs show exactly 1,000,000 input ratings, plus 87,585 lookup movies. A slice is permitted by the assignment. |
| Usable event-time column | UNIX seconds in timestamp; observed source range 1996-02-28 to 2023-10-12 UTC. |
| Dataset claimed in course forum, not another team's | Cannot verify from repository; team must confirm the claim. |
| Proposal <=2 pages and required contents | Proposal PDF has exactly two pages and states questions, dataset, roles, storage, deep dive and risks. Its submission is not verifiable from the repository. |
| Checkpoint code ZIP and <=3-minute recording | Not verified as submitted. A checkpoint recording is mentioned in .gitignore, but not supplied as a tracked artifact. |
| Final report <=8 pages plus optional appendix | Complete eight-page PDF rendered and checked; rubric sections are included. |
| Final report design, DB alternative, changes | Complete: architecture diagram, SQLite/PostgreSQL comparison, proposal deltas and reasons. |
| Evidence for all parts and before/after deep dive | Captured storage, batch, quality, watermark replay, CSV oracle, three timed database answers and 30-trial index measurements are included. |
| 100x-data discussion, contributions, AI acknowledgement | Included in PDF; team should review attribution and add any other AI tools used. |
| Final code ZIP | Rebuilt 25 Nov with source, input data, report and SHA-256 manifest; ZIP integrity checked. Local changes remain uncommitted/unpublished to GitHub. |
| Final 6–8-minute recording | Team must record actual live results, batch, stream, database arrivals, all three queries, a hand-added bad row with counts, and deep-dive measurements; every member must speak. |
| Individual peer reviews | Each teammate must submit their own review; cannot verify from code. |

No exactly-once guarantee, cloud account, cluster, or Kubernetes is required by the assignment. The proposal's original five-restart success criterion (now reduced to 2-3 per professor feedback) is stronger than the general minimum. Two restarts were measured against the previous 15-minute generation; repeat them against `grace5` before claiming restart evidence for the current stream code.

## Validation fixes discovered at runtime

The first bind-mounted SQLite run failed with a malformed-database error during the hourly sink. The database was being opened from macOS and Linux concurrently. SQLite now resides only in a Docker-managed volume; all live queries and validation must run inside a container. The failed bind-mounted database was retained under data/ as diagnostic evidence, not reused. Kafka persistence also required its image-owned /var/lib/kafka/data directory, rather than an unowned new /tmp directory. The prior replay used `analytics-data` and `stream_sqlite`; the current 5-minute implementation has isolated `analytics-grace5`, `kafka-grace5` and `stream_grace5` state. Late injection waits for the current run's published event watermark.

Spark progress `numInputRows` for the independent quality/late foreachBatch queries can include repeated reads caused by multiple actions. For exact source-ingress counts, use Kafka endOffset minus startOffset; the explicit quality stage good/bad counts and database aggregate counts retain their stated meanings.

## Current captured runtime outcome (five-minute implementation; 25 November)

The isolated Docker-volume run processed 1,000,005 Kafka rows: 1,000,000 source ratings were included, five deliberately late rows were stored in SQLite `late_events`, and zero rows failed quality. The incoming = included + late + quality equation passed per batch and in total. SQLite integrity returned `ok`; the independent CSV oracle passed all three answers and all 994,006 hourly/movie keys. The API health, all three answer routes and demo status returned HTTP 200, and every query returned a measured `elapsed_ms`. See `watermark_validation.json`, `validation.json`, `evidence/csv_verification.log`, and the PDF for values.


## Earlier report-session validation (superseded by five-minute classifier)

Two consumer restarts completed under the prior 15-minute implementation. That run observed 1,000,007 total events and dropped its seven test rows. These results remain historical evidence only; the new version uses a fresh SQLite/Kafka volume namespace and checkpoint generation.

The PDF uses final_index_benchmark.json for the frozen 30-trial measurements; earlier timing artifacts are separate runs. The report is eight pages. Confirmed contributions: Nafisa Serve/checkpoint video; Izma Store/Batch; Homa Stream and <=3-command startup; Nitant Quality/Monitoring.

Current coverage limits: no forced process restart has yet been performed against the new `grace5` checkpoint/database/Kafka generation. Sink retry and rollback are unit-tested, but are not a substitute for process-restart evidence. There is no global source-event deduplication across fresh Kafka offsets. Manual final video and peer-review submissions also cannot be verified from repository code.

## Five-minute watermark implementation and validation added 25 November 2026

`consumer.py` now uses one Kafka `foreachBatch` query so raw incoming rows, quality failures, accepted rows, and late rows are counted from the same offsets. It processes the single Kafka partition in offset order. The persisted high-water mark is the greatest valid event timestamp observed so far. Each event window remains eligible while `high_water_mark <= window_end + 300 seconds`; a row is set aside only after that inclusive boundary. In-grace window counts are added as deltas, allowing a previous hour to update during its grace period. Late rows are stored atomically in SQLite `late_events`; batch totals and the high-water mark are committed in `stream_batches` with the hourly counts and idempotent batch marker.

The invariant checked per batch is `raw incoming = within-grace + late-quarantined + quality-failed`; also `valid incoming = within-grace + late-quarantined`. The live `/demo/status` response and dashboard show these counts, conservation status, watermark, and samples from the late-row table. `tests/test_watermark.py` covers the exact five-minute boundary, row-level quarantine, idempotent retry, row-count mismatch rollback, and conservation totals. The full Docker replay has been run after this change: 1,000,005 in, 1,000,000 included, five quarantined late, zero quality failures; all hourly keys and answers match the independent CSV oracle. The full 28-test suite passes inside the batch container. The current implementation still needs a forced restart experiment against its new checkpoint generation.
