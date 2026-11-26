"""Capture and assert the isolated five-minute watermark replay results."""
import json
from pathlib import Path

from database import connect
from observability import snapshot


def capture():
    status = snapshot()
    with connect() as db:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        batches = [dict(row) for row in db.execute(
            "SELECT batch_id,rows_in,valid_rows,quality_failed,within_grace,late_rows,max_event_time,watermark "
            "FROM stream_batches ORDER BY batch_id"
        )]
        late_rows = [dict(row) for row in db.execute(
            "SELECT source_partition,source_offset,batch_id,user_id,movie_id,rating,event_timestamp,"
            "window_start,window_end,watermark_at_arrival,late_seconds,row_json "
            "FROM late_events ORDER BY source_partition,source_offset"
        )]
        for row in late_rows:
            row["row"] = json.loads(row.pop("row_json"))

    oracle_path = Path("reports/validation.json")
    oracle = json.loads(oracle_path.read_text()) if oracle_path.exists() else None
    late = status["late_arrivals"]
    batch_conservation = all(
        row["rows_in"] == row["within_grace"] + row["late_rows"] + row["quality_failed"]
        and row["valid_rows"] == row["within_grace"] + row["late_rows"]
        for row in batches
    )
    result = {
        "database_integrity": integrity,
        "rows": late,
        "late_rows": late_rows,
        "stream_batches": batches,
        "batch_conservation_ok": batch_conservation,
        "csv_oracle": oracle,
        "answer_timings_ms": {
            str(answer["question"]): answer["elapsed_ms"]
            for answer in (oracle or {}).get("answers", [])
        },
    }
    output = Path("reports/watermark_validation.json")
    output.write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(json.dumps({
        "database_integrity": integrity,
        "rows": late,
        "batch_count": len(batches),
        "batch_conservation_ok": batch_conservation,
        "csv_checks": (oracle or {}).get("checks"),
        "evidence_file": str(output),
    }, indent=2))
    if integrity != "ok" or not late["conservation_ok"] or not batch_conservation:
        raise SystemExit("watermark replay evidence failed an integrity/conservation check")
    if oracle and not all(oracle.get("checks", {}).values()):
        raise SystemExit("CSV oracle failed; inspect reports/validation.json")


if __name__ == "__main__":
    capture()
