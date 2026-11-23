import os
import argparse
import json
import time

import pyarrow.dataset as ds
from kafka import KafkaProducer

from quality import alert_if_needed, stage

# Read command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--start-year", type=int, default=1996)
parser.add_argument("--end-year", type=int, default=2023)
parser.add_argument("--max-messages", type=int, default=None)
parser.add_argument(
    "--inject-late",
    type=int,
    default=0,
    help="After the main send, re-send this many earliest ratings (late arrival demo)",
)
args = parser.parse_args()

if args.start_year > args.end_year:
    parser.error("start-year cannot be greater than end-year")

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
failed = 0
total_selected = 0
late_seed = []


def validate(rating):
    fails = []
    if (
        rating.get("userId") is None
        or rating.get("movieId") is None
        or rating.get("rating") is None
        or rating.get("timestamp") is None
    ):
        fails.append("missing")
        return fails
    if not (0.5 <= float(rating["rating"]) <= 5.0):
        fails.append("range")
    if (
        int(rating["userId"]) <= 0
        or int(rating["movieId"]) <= 0
        or int(rating["timestamp"]) <= 0
    ):
        fails.append("ids")
    if seen is not None:
        key = (rating["userId"], rating["movieId"], rating["timestamp"])
        if key in seen:
            fails.append("duplicates")
        else:
            seen.add(key)
    return fails


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

                producer.send("ratings", value=rating)
                sent += 1

            producer.flush(timeout=120)

            if sent > 0 and sent % 100000 == 0:
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
        print("Waiting 20s for stream watermark to advance...", flush=True)
        time.sleep(20)
        print(f"Injecting {late_n:,} late ratings (event_time -24h)...", flush=True)
        for rating in late_seed[:late_n]:
            late_row = dict(rating)
            late_row["timestamp"] = int(rating["timestamp"]) - 24 * 3600
            producer.send("ratings", value=late_row)
        producer.flush(timeout=60)
        print(f"Finished late inject: {late_n:,} ratings.", flush=True)

finally:
    producer.close()
