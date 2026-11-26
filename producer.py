import os
import argparse
import json
import time
import math
from datetime import datetime

import pyarrow.dataset as ds
from kafka import KafkaProducer

from quality import alert_if_needed, row_failures, stage

# Read command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--start-year", type=int, default=1996)
parser.add_argument("--end-year", type=int, default=2023)
parser.add_argument("--max-messages", type=int, default=None)
parser.add_argument("--rate-per-second", type=float, default=0,
                    help="Pace real replay for recording (0 sends as fast as possible)")
parser.add_argument("--progress-every", type=int, default=100000)
parser.add_argument(
    "--inject-late",
    type=int,
    default=0,
    help="After the main send, re-send this many earliest ratings (late arrival demo)",
)
args = parser.parse_args()

if args.start_year > args.end_year:
    parser.error("start-year cannot be greater than end-year")
if not math.isfinite(args.rate_per_second) or args.rate_per_second < 0 or args.progress_every <= 0:
    parser.error("rate-per-second must be finite/nonnegative and progress-every must be positive")
if (args.max_messages is not None and args.max_messages < 0) or args.inject_late < 0:
    parser.error("max-messages and inject-late must be nonnegative")

dataset = ds.dataset(
    "ratings.parquet",
    format="parquet",
    partitioning="hive",
)

# Full 32M cannot keep a 32M-key duplicate set in RAM; batch stage already checked dups.
track_dups = args.max_messages is not None
seen = set() if track_dups else None

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    linger_ms=50,
    batch_size=32768,
)

start_time = time.time()
sent = 0
last_progress = 0
failed = 0
total_selected = 0
late_seed = []


def validate(rating):
    return row_failures(rating, seen)


try:
    # Year-by-year keeps peak memory down while preserving event-time order.
    for year in range(args.start_year, args.end_year + 1):
        if args.max_messages is not None and total_selected >= args.max_messages:
            break

        table = dataset.to_table(
            filter=ds.field("year") == year,
            columns=["userId", "movieId", "rating", "timestamp"],
        )
        table = table.sort_by([("timestamp", "ascending")])

        if args.max_messages is not None:
            remaining = args.max_messages - total_selected
            if remaining <= 0:
                break
            if table.num_rows > remaining:
                table = table.slice(0, remaining)

        year_rows = table.num_rows
        total_selected += year_rows
        print(f"Year {year}: {year_rows:,} ratings", flush=True)

        for batch in table.to_batches(max_chunksize=1000):
            pending = []
            for rating in batch.to_pylist():
                if len(late_seed) < max(args.inject_late, 0):
                    late_seed.append(dict(rating))

                fails = validate(rating)
                if fails:
                    failed += 1
                    with open(
                        "bad_ratings_producer.jsonl", "a", encoding="utf-8"
                    ) as handle:
                        handle.write(json.dumps({"row": rating, "fails": fails}) + "\n")
                    continue

                # Keep a bounded group of futures; check broker acknowledgements.
                pending.append(producer.send("ratings", value=rating))
                sent += 1

            producer.flush(timeout=120)
            for future in pending:
                future.get(timeout=120)
            if args.rate_per_second:
                time.sleep(max(0, sent / args.rate_per_second - (time.time() - start_time)))

            if sent - last_progress >= args.progress_every:
                last_progress = sent
                elapsed = time.time() - start_time
                rate = sent / elapsed if elapsed else 0
                print(
                    f"Sent {sent:,} ratings (failed={failed:,}) "
                    f"in {elapsed:.1f}s ({rate:,.0f}/s)",
                    flush=True,
                )

    print(
        f"Finished sending {sent:,} ratings (selected={total_selected:,}).",
        flush=True,
    )
    stage("producer", total_selected, sent, failed)
    alert_if_needed(failed, total_selected, "producer")

    if args.inject_late > 0 and late_seed:
        late_n = min(args.inject_late, len(late_seed))
        print("Waiting for the 5-minute event-time watermark before late injection...", flush=True)
        # A fixed sleep races first-run Spark startup. Wait until this replay has
        # advanced the custom window-end watermark beyond each old test row.
        required = max(
            ((int(r["timestamp"]) - 24 * 3600) // 3600 + 1) * 3600
            for r in late_seed[:late_n]
        )
        deadline = time.monotonic() + 600
        ready = False
        while time.monotonic() < deadline and not ready:
            try:
                with open("reports/event_watermark.json", encoding="utf-8") as handle:
                    progress = json.load(handle)
                ready = (progress.get("updated_at_epoch", 0) >= start_time
                         and progress.get("watermark_epoch", 0) > required)
            except (ValueError, OSError):
                pass
            if not ready:
                time.sleep(2)
        if not ready:
            raise RuntimeError("No confirmed five-minute watermark advancement; refusing invalid late-event injection")
        print(f"Injecting {late_n:,} late ratings (event_time -24h)...", flush=True)
        for rating in late_seed[:late_n]:
            late_row = dict(rating)
            late_row["timestamp"] = int(rating["timestamp"]) - 24 * 3600
            producer.send("ratings", value=late_row).get(timeout=60)
        producer.flush(timeout=60)
        print(f"Finished late inject: {late_n:,} ratings.", flush=True)
        stage("producer-late-test", late_n, late_n, 0)

finally:
    producer.close()
