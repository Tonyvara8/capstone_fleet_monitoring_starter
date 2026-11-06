"""Part C.1 -- daily data-quality job"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from spark_jobs.batch_common import DEFAULT_EVENTS_PATH, DEFAULT_QUALITY_DIR, create_batch_session, read_day

MIN_ROWS = 100
MAX_DUPLICATE_PCT = 0.5
MAX_UNKNOWN_PCT = 2.0


def compute_quality(events, min_rows=MIN_ROWS, max_duplicate_pct=MAX_DUPLICATE_PCT, max_unknown_pct=MAX_UNKNOWN_PCT):
    """events: DataFrame of one day. Returns a dict (metrics, thresholds, passed, reasons)"""
    per_id = events.groupBy("event_id").count()
    row = events.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("region") == "UNKNOWN", 1).otherwise(0)).alias("unknown_rows"),
    ).first()
    dup = per_id.agg(
        F.sum(F.when(F.col("count") > 1, 1).otherwise(0)).alias("duplicated_event_ids"),
        F.sum(F.when(F.col("count") > 1, F.col("count") - 1).otherwise(0)).alias("duplicate_rows"),
    ).first()

    rows = int(row["rows"] or 0)
    unknown_rows = int(row["unknown_rows"] or 0)
    duplicated_ids = int(dup["duplicated_event_ids"] or 0)
    duplicate_rows = int(dup["duplicate_rows"] or 0)
    duplicate_pct = round(100.0 * duplicate_rows / rows, 4) if rows else 0.0
    unknown_pct = round(100.0 * unknown_rows / rows, 4) if rows else 0.0

    reasons = []
    if rows < min_rows:
        reasons.append(f"too few rows: {rows} < {min_rows}")
    if duplicate_pct > max_duplicate_pct:
        reasons.append(f"duplicate rate {duplicate_pct}% > {max_duplicate_pct}%")
    if unknown_pct > max_unknown_pct:
        reasons.append(f"UNKNOWN region rate {unknown_pct}% > {max_unknown_pct}%")

    return {
        "rows": rows,
        "duplicated_event_ids": duplicated_ids,
        "duplicate_rows": duplicate_rows,
        "duplicate_pct": duplicate_pct,
        "unknown_region_rows": unknown_rows,
        "unknown_region_pct": unknown_pct,
        "thresholds": {"min_rows": min_rows, "max_duplicate_pct": max_duplicate_pct, "max_unknown_pct": max_unknown_pct},
        "passed": not reasons,
        "reasons": reasons,
    }


def write_json_atomically(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (event_date partition)")
    parser.add_argument("--events-path", default=str(DEFAULT_EVENTS_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_QUALITY_DIR))
    parser.add_argument("--min-rows", type=int, default=MIN_ROWS)
    args = parser.parse_args(argv)

    spark = create_batch_session("FleetQualityJob")
    try:
        events = read_day(spark, args.events_path, args.date)
        verdict = compute_quality(events, min_rows=args.min_rows)
        verdict["date"] = args.date
        out = Path(args.output_dir) / args.date / "verdict.json"
        write_json_atomically(out, verdict)
        print(json.dumps(verdict, indent=2, sort_keys=True))
        print(f"verdict written to {out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
