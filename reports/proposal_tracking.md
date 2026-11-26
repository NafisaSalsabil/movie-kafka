# Proposal-to-implementation tracking

Confirmed contributions: Nafisa Salsabil - Serve and checkpoint video; Izma Mujeeb - Store/Batch; Homa Ahmadinoori - Stream and Docker startup in <=3 commands; Nitant Chandulal Suhagiya - Quality/Monitoring.

| Proposal | Current change | Reason / evidence |
|---|---|---|
| MovieLens 32M | Bundled one-million-rating slice | Laptop scope; assignment explicitly permits >=1M rows. Batch evidence confirms one million. |
| Parquet by rating year | Retained for ratings; unpartitioned movies | Movie lookup has no rating event year. |
| Common serving database | SQLite with WAL and ranking indexes | Embedded laptop deployment; PostgreSQL comparison in PDF. |
| Stream event-time windows | 1h UTC windows + 5m event-time watermark | Inclusive window-end grace; exact cutoff tests added. Latest stored event hour is Q3. |
| Idempotent sink | Additive keyed updates + atomic batch ledger | Grace-period arrivals can revise stored hours; the ledger prevents replayed microbatches from double-counting. |
| Persisted recovery | Spark checkpoints + Kafka volume + SQLite | All must stay consistent. |
| Quality | Four checks, separate quarantine paths, >1% alert | Null classification corrected; stage counts logged. |
| 2-3 forced consumer restarts (professor-adjusted) | Two completed SIGKILL/restarts captured | Resumed reads and retained open-window state; active-write crash not established. |
| Late-event reconciliation | Offset-ordered watermark classification and SQLite `late_events` table | Exact five-minute window-end cutoff, source offsets retained, and per-batch conservation enforced. |

Git contains one baseline commit, 5442e1c (Initial project commit). The change inventory compares the working tree against that baseline; Git does not establish who authored each uncommitted change. The report and evidence scripts were added in this session. AI usage beyond this Codex session remains unconfirmed.

## Runtime changes from concurrent serving completion

- SQLite moved from the macOS project bind mount to Docker-managed volumes, exposed to drivers as `/database/analytics.sqlite3`. The initial bind-mount run raised a malformed-database error; cross-platform locking was a likely cause, not conclusively established. The failed database is diagnostic only.
- The current implementation uses isolated `analytics-grace5` and `kafka-grace5` volumes and a fresh `checkpoints/stream_grace5` generation. Do not mix old offsets or counts with the new ledger.
- Late injection waits for current-run hourly watermark progress, replacing the fixed 20-second delay that could inject before the consumer caught up.
- `batch_database.py` reloads batch aggregates from existing Parquet without rewriting stored source data.
- Kafka logs use the image-owned `/var/lib/kafka/data` location. `KAFKA_HOST_PORT` avoids a collision with existing port 9092.

Final evidence artifacts should be generated from this repaired run; earlier partial or failed runs are labelled as such.

Instructor-approved scope update, confirmed by the user in this report session: use 2-3 forced consumer restarts instead of the original five. Retain a during-processing restart and verify recovered counts.


## Earlier report-session validation (prior 15-minute implementation)

The captured restarts, seven dropped probes, and CSV oracle ran against the earlier 15-minute watermark code. They do not stand as runtime evidence for the new 5-minute implementation.

The PDF uses final_index_benchmark.json for the frozen 30-trial measurements; earlier timing artifacts are separate runs. The report is eight pages. Confirmed contributions: Nafisa Serve/checkpoint video; Izma Store/Batch; Homa Stream and <=3-command startup; Nitant Quality/Monitoring.

Remaining coverage limits: Docker replay and restart evidence must be regenerated for the new implementation; there is no global source-event deduplication across fresh Kafka offsets. The new unit tests cover the exact boundary and SQLite rollback/replay behavior.

## Five-minute watermark update

The consumer now classifies the single Kafka partition in offset order. For a row in `[window_start, window_end)`, the row is included if the greatest earlier valid event timestamp is at most `window_end + 300`; later rows are stored in SQLite `late_events`. Each transaction records raw input, quality failures, included rows, late rows, maximum event time, and hourly deltas. The checked conservation equations are `valid = included + late` and `raw = included + late + quality_failed`. The dashboard exposes counts and late-row samples. Unit tests passed locally; Docker replay evidence is pending.
