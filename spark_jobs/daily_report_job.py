"""Part C.2 -- daily report job"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark_jobs.batch_common import (
    DEFAULT_EVENTS_PATH, DEFAULT_REPORT_DIR, aggregate_plain, aggregate_salted, create_batch_session, read_day,
    scanned_directories, worst_vehicles,
)
from spark_jobs.quality_job import write_json_atomically


def build_report(events, date, salting=0):
    """events: one day of enriched events. salting=0 -> plain aggregation, N>0 -> two-stage with N salt buckets"""
    aggregate = (lambda key: aggregate_salted(events, key, salting)) if salting else (lambda key: aggregate_plain(events, key))
    per_vehicle = aggregate("vehicle_id").cache()
    per_region = aggregate("region")

    vehicles = [row.asDict() for row in per_vehicle.orderBy("vehicle_id").collect()]
    regions = [row.asDict() for row in per_region.orderBy("region").collect()]
    worst = [row.asDict() for row in worst_vehicles(per_vehicle).collect()]
    per_vehicle.unpersist()
    return {
        "date": date,
        "total_events": sum(v["events"] for v in vehicles),
        "per_vehicle": vehicles,
        "per_region": regions,
        "worst_vehicles": worst,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (event_date partition)")
    parser.add_argument("--events-path", default=str(DEFAULT_EVENTS_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--salting", type=int, default=0, help="number of salt buckets (0 = no salting)")
    parser.add_argument("--explain", action="store_true", help="print the physical plan (shows PartitionFilters)")
    parser.add_argument("--ui-pause", type=int, default=0, help="keep the Spark UI alive N seconds at the end")
    args = parser.parse_args(argv)

    spark = create_batch_session("FleetDailyReportJob")
    try:
        events = read_day(spark, args.events_path, args.date)
        if args.explain:
            events.explain(mode="formatted")
            print(f"event_date directories actually scanned: {scanned_directories(events)}", flush=True)
        report = build_report(events, args.date, args.salting)
        out = Path(args.output_dir) / args.date / "report.json"
        write_json_atomically(out, report)
        print(f"report written to {out}: {report['total_events']} events, {len(report['per_vehicle'])} vehicles")
        for row in report["worst_vehicles"]:
            print("worst:", row)
        if args.ui_pause:
            import time
            print(f"Spark UI at {spark.sparkContext.uiWebUrl} for {args.ui_pause}s", flush=True)
            time.sleep(args.ui_pause)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
