import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from spark_jobs.batch_common import (
    aggregate_plain, aggregate_salted, join_profile, join_profile_salted, read_day, scanned_directories, worst_vehicles,
)
from spark_jobs.daily_report_job import build_report
from spark_jobs.quality_job import compute_quality, write_json_atomically
from spark_jobs.quality_job import main as quality_main

SCHEMA = ("event_id STRING, vehicle_id STRING, event_time TIMESTAMP, speed_kmh DOUBLE, harsh_braking BOOLEAN, "
          "region STRING")


def row(event_id, vehicle="V0001", speed=60.0, braking=False, region="Paris", day=5, hour=10):
    return (event_id, vehicle, datetime(2026, 11, day, hour, 0, 0), float(speed), braking, region)


class BatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("batch-test")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4")
                     .config("spark.sql.session.timeZone", "UTC")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_quality_passes_on_clean_data_and_fails_on_each_rule(self):
        clean = self.spark.createDataFrame([row(str(i)) for i in range(200)], SCHEMA)
        self.assertTrue(compute_quality(clean)["passed"])

        few = self.spark.createDataFrame([row(str(i)) for i in range(10)], SCHEMA)
        self.assertIn("too few rows", compute_quality(few)["reasons"][0])

        dups = self.spark.createDataFrame([row(str(i)) for i in range(200)] + [row("1"), row("1"), row("2")], SCHEMA)
        verdict = compute_quality(dups)
        self.assertEqual((verdict["duplicated_event_ids"], verdict["duplicate_rows"]), (2, 3))
        self.assertFalse(verdict["passed"])

        unknown = self.spark.createDataFrame(
            [row(str(i)) for i in range(190)] + [row(f"u{i}", vehicle="V0099", region="UNKNOWN") for i in range(10)], SCHEMA)
        verdict = compute_quality(unknown)
        self.assertEqual(verdict["unknown_region_pct"], 5.0)
        self.assertFalse(verdict["passed"])

    def test_quality_boundaries_are_inclusive(self):
        rows = [row(str(i)) for i in range(196)] + [row(f"u{i}", region="UNKNOWN") for i in range(4)]
        verdict = compute_quality(self.spark.createDataFrame(rows, SCHEMA))
        self.assertEqual(verdict["unknown_region_pct"], 2.0)
        self.assertTrue(verdict["passed"])

    def test_salted_aggregation_equals_plain_aggregation(self):
        rows = [row(str(i), vehicle="V0001" if i % 3 else f"V{i % 7 + 2:04d}", speed=40 + (i % 90),
                    braking=(i % 17 == 0)) for i in range(600)]
        events = self.spark.createDataFrame(rows, SCHEMA)
        for key in ("vehicle_id", "region"):
            plain = sorted((tuple(r.asDict().items()) for r in aggregate_plain(events, key).collect()), key=str)
            salted = sorted((tuple(r.asDict().items()) for r in aggregate_salted(events, key, 8).collect()), key=str)
            self.assertEqual(plain, salted)

    def test_salted_join_equals_plain_join(self):
        events = self.spark.createDataFrame([row(f"e{i}", vehicle="V0001" if i % 3 else "V0002") for i in range(300)]
                                            + [row("x", vehicle="V0099")], SCHEMA)
        profile = self.spark.createDataFrame([("V0001", "AA", "van"), ("V0002", "BB", "truck")],
                                             "vehicle_id STRING, plate STRING, vehicle_type STRING")
        plain = sorted((r.event_id, r.plate) for r in join_profile(events, profile).collect())
        salted = sorted((r.event_id, r.plate) for r in join_profile_salted(events, profile, 5).collect())
        self.assertEqual(plain, salted)
        self.assertEqual(len(salted), 301)

    def test_worst_vehicles_ranking(self):
        rows = []
        for vehicle, speeding, braking in [("V1", 5, 0), ("V2", 1, 1), ("V3", 0, 4), ("V4", 0, 0), ("V5", 2, 2), ("V6", 9, 9)]:
            rows += [row(f"{vehicle}-s{i}", vehicle, 130) for i in range(speeding)]
            rows += [row(f"{vehicle}-b{i}", vehicle, 50, braking=True) for i in range(braking)]
            rows.append(row(f"{vehicle}-ok", vehicle, 50))
        events = self.spark.createDataFrame(rows, SCHEMA)
        worst = [r.vehicle_id for r in worst_vehicles(aggregate_plain(events, "vehicle_id"), 5).collect()]
        self.assertEqual(worst, ["V6", "V1", "V5", "V3", "V2"])

    def test_report_job_reads_only_the_requested_partition_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "events")
            rows = [row(f"a{i}", day=5) for i in range(30)] + [row(f"b{i}", day=6) for i in range(20)]
            events = self.spark.createDataFrame(rows, SCHEMA)
            events = events.withColumn("event_date", events.event_time.cast("date"))
            events.withColumn("batch_id", events.speed_kmh.cast("int") * 0).write.partitionBy("event_date", "batch_id").parquet(path)

            day = read_day(self.spark, path, "2026-11-05")
            plan = day._jdf.queryExecution().executedPlan().toString()
            self.assertIn("PartitionFilters", plan)
            self.assertEqual(scanned_directories(day), ["2026-11-05"])

            first = build_report(day, "2026-11-05")
            second = build_report(read_day(self.spark, path, "2026-11-05"), "2026-11-05", salting=4)
            self.assertEqual(first["total_events"], 30)
            self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True))

    def test_verdict_file_is_written_atomically_and_rewritten_identically(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "2026-11-05" / "verdict.json"
            write_json_atomically(target, {"passed": True})
            first = target.read_bytes()
            write_json_atomically(target, {"passed": True})
            self.assertEqual(first, target.read_bytes())
            self.assertEqual([p.name for p in target.parent.iterdir()], ["verdict.json"])


if __name__ == "__main__":
    unittest.main()
