# Final recording: 7 minutes 30 seconds

The recording must be 6–8 minutes, include every member's voice, and show results produced by the running system. This guide targets **7:30**. Cuts may remove startup/waiting time. Capture actual execution for each scene; read the numbers that appear during your take.

## Rehearse in an isolated copy

Use a fresh copy of this completed project for each full take. Docker Desktop must be running. From that copy:

```sh
python3 demo.py start
```

The standard-library host script uses Docker for all processing. It starts a separate `movie-reviews-recording-<run-id>` project, with its own Kafka and SQLite volumes, generated files in `recording-runtime/`, Kafka port 19093, and dashboard at **http://localhost:8001/demo**. It preserves the standard project's evidence. Use only one recording project at a time because recording ports are fixed. A unique project name is saved in `recording-runtime/project.json` and reused by every command. Startup refuses to replay into an existing recording directory. To begin another take, stop the old recording project and use a fresh project copy without `recording-runtime/`; the script assigns fresh volumes automatically. You can also retain this run and cut between captured scenes. Never delete only the checkpoints or restart the producer into the same stream.

Run this immediately in a second terminal, while source replay is still active:

```sh
python3 demo.py recover --restarts 3
```

The first crash waits for a nonempty hourly Spark offset log whose batch has no commit file, with the producer active. The harness sends SIGKILL to the recording consumer, restarts that same service without its dependencies, and measures recovery until a different Spark run ID publishes hourly progress. It repeats three times (the approved target is 2–3). Keep this terminal in the recorded scenes. Do not restart other services by hand during the experiment.

In another terminal, show real batch/producer/consumer output:

```sh
python3 demo.py logs
```

The producer acknowledges Kafka sends, prints progress every 10,000 ratings, and paces the real one-million-row replay at up to 3,500 rows/second. Playback is accelerated across 1996–2023; this is not the current wall-clock hour. All default source rows still run through the system.

## Capture these scenes

| Time | Speaker | Live action and visible evidence |
|---|---|---|
| 0:00–0:25 | Izma | Introduce all members and trace CSV → Spark batch → Parquet → Kafka → Spark windows → SQLite. State that the real MovieLens slice has one million ratings. |
| 0:25–1:30 | Izma | Show the actual batch terminal executing, four quality counts, year-partitioned Parquet writes, then batch-complete/database counts on the Pipeline tab. A finished log alone is insufficient to show the job running: include captured execution. |
| 1:30–2:35 | Homa | Show producer row counts increasing, Kafka offset advancement, and at least two different database-arrival totals on the Pipeline tab. Explain one-hour UTC windows and the five-minute window-end watermark: on-time rows update the hourly total; later rows go to SQLite `late_events`. Show incoming, included, late, quality-failed counts and the conservation pass. Show the crash command during active processing; cuts may join the restart scenes. |
| 2:35–4:05 | Nafisa | On Three questions, click all three buttons. Read the actual leading result and SQL timing for each. Expand Executed SQL. Q1 uses ≥1,000 ratings; Q2 includes all genres and tied winners; Q3 uses the latest stored event-time hour. Show the hour boundaries in Q3. |
| 4:05–5:10 | Nitant | Show baseline quality counts. Type the bad JSON by hand, send it, show its Kafka acknowledgement/offset, wait for the consumer quarantine sample with that same offset, then show the range-failure and rejected-row counts increase. Show the quality alert in logs if the bad row is isolated in a microbatch. |
| 5:10–6:40 | Homa | On Deep part, show all three measured recovery times, before/after counts, active input batch evidence, and full CSV reconciliation. Show five late test events dropped by Spark, with the unchanged source total. Explain why counts and replay marker commit together. |
| 6:40–7:30 | Nafisa, then all | Show measured index before/after results as the additional experiment, mention warm-cache/snapshot limits. Each member briefly names their own contribution. Close within 8:00. |

All four members already have substantive speaking sections. Confirm voices are audible in the final exported file; code cannot establish who spoke.

## Hand-added bad row

On **Hand-added bad row**, type this JSON into the initially empty field during the recording:

```json
{"userId":999999999,"movieId":1,"rating":9.0,"timestamp":1697148000}
```

Click **Send entered row to Kafka** once. This deliberately bypasses producer validation so the actual Spark consumer must catch the invalid rating. The API returns a Kafka offset only after broker acknowledgement. The page later confirms that exact offset in the consumer's quarantine sample. A range-invalid row cannot enter hourly aggregation, regardless of its timestamp. Do not claim a scripted fixture alone meets the hand-added requirement.

Before/after quality counts should show input +1, rejected +1, range failures +1, and accepted unchanged for this row. Other live traffic may also increase input/accepted counts. For the clearest isolated +1 comparison, inject after source replay and catch-up; you may cut from an earlier streaming scene.

## Finish and verify the numerical evidence

After restart recovery and the hand-added bad row:

```sh
python3 demo.py verify
python3 demo.py benchmark
python3 demo.py check
```

`verify` waits for producer success, source-count catch-up, and five rows recorded in `late_events`, then independently checks all three answers, the source total, and every hourly movie key against the CSV. It promotes the restart evidence to passed only after all required recoveries and that CSV check. `benchmark` runs 30 trials per question per index state on a SQLite backup taken inside Linux; it never drops indexes on the live database. `check` fails if any software-backed recording requirement is missing and prints the actual three answers. The dashboard's Pipeline tab shows the conservation equation; its Deep part tab shows sample late rows and their offsets.

Evidence lives under `recording-runtime/reports/`: `storage_sizes.json`, `stream_progress.jsonl`, `restart_validation.json`, `validation.json`, `recording_index_benchmark.json`, and `recording_readiness.json`. Quality counts and samples also persist in the recording project's SQLite database. They replace a replayed batch's counts so a retry cannot double-count the quality totals; the Parquet diagnostic quarantine remains append-only.

## Export and submit

- Watch the final exported recording and confirm its duration is 6:00–8:00, all four voices appear, every required result is readable, and terminal/UI evidence comes from captured execution.
- Submit the recording with the report. If too large for the dropbox, place it in your TMU Google Drive, configure access so anyone at TMU can view, and submit that link as required by the assignment. Confirm access using a second TMU account.
- Check that report numbers refer to the same captured run, or clearly identify a separately measured run. Existing report artifacts are not automatically rewritten by recording mode.
- Preserve evidence and stop only the recording services with `python3 demo.py stop` when finished. Stopping preserves Kafka, database, checkpoints, and evidence.

This implements and verifies the system outputs. The team still creates the actual audiovisual recording, speaks, reviews its duration, and submits it.
