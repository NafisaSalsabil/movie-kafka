# Final report status

Complete: eight-page PDF, code/input/evidence ZIP, confirmed contributions and AI acknowledgement.

## Five-minute watermark validation

The current `grace5` run processed 1,000,005 Kafka rows: 1,000,000 source ratings were included, five deliberately late rows were retained in SQLite `late_events`, and zero rows failed quality checks. Per-batch and total row conservation passed, all 994,006 hourly aggregate keys matched the independent CSV oracle, all three serving questions passed, and SQLite `integrity_check` returned `ok`. Full row samples, watermark values, batch counts, timings and oracle results are in `watermark_validation.json`.

The five-minute boundary is inclusive: an event is accepted when the prior maximum event time is no later than `window_end + 300 seconds`; it is quarantined only when the maximum is strictly later. Boundary behavior and transactional retries are covered by `tests/test_watermark.py`.

Earlier two forced-restart measurements in `restart_validation.json` were captured against the previous watermark/checkpoint generation. They are historical and are not evidence of restart behavior for the current `grace5` generation. The current generation's sink idempotence and transaction rollback are unit-tested; a forced process restart against the new generation remains unmeasured. No claim of global source-event deduplication across fresh Kafka offsets is made.

The PDF uses `final_index_benchmark.json` for the 30-trial frozen index measurements. Earlier timing artifacts are separate runs. The report contains eight pages and the confirmed contribution table: Nafisa implemented Serve and made the checkpoint video; Izma implemented Store/Batch; Homa implemented Stream and <=3-command startup; Nitant implemented Quality/Monitoring.
