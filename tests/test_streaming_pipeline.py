"""End-to-end check of the streaming job WITHOUT Kafka"""
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs import fleet_streaming as fs

BASE = "2026-10-20T10:{m:02d}:{s:02d}.000+00:00"


def event(event_id, vehicle, minute, second=0, speed=80.0, braking=False, hour=10):
    return json.dumps({
        "event_id": event_id, "vehicle_id": vehicle,
        "event_time": f"2026-10-20T{hour:02d}:{minute:02d}:{second:02d}.000+00:00",
        "latitude": 48.8, "longitude": 2.3, "speed_kmh": speed, "fuel_level_pct": 60.0, "harsh_braking": braking,
    })


class StreamingPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("pipeline-test")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        (self.tmp / "in").mkdir()
        self.published = []
        self._saved = (fs.STREAMING_CHECKPOINT_ROOT, fs.PARQUET_EVENTS_PATH, fs.publish_alerts)
        fs.STREAMING_CHECKPOINT_ROOT = self.tmp / "ckpt"
        fs.PARQUET_EVENTS_PATH = self.tmp / "parquet"
        fs.publish_alerts = lambda messages: self.published.extend(json.loads(r.value) for r in messages.collect())

    def tearDown(self):
        fs.STREAMING_CHECKPOINT_ROOT, fs.PARQUET_EVENTS_PATH, fs.publish_alerts = self._saved
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write_file(self, name, lines):
        (self.tmp / "in" / name).write_text("\n".join(lines) + "\n")

    def raw_stream(self):
        raw = self.spark.readStream.schema("value STRING").option("maxFilesPerTrigger", 1).text(str(self.tmp / "in"))
        return raw.select(
            F.lit("k").alias("source_key"), F.col("value").alias("raw_value"), F.lit("vehicle_events").alias("source_topic"),
            F.lit(0).alias("source_partition"), F.lit(0).cast("long").alias("source_offset"),
            F.current_timestamp().alias("source_timestamp"))

    def catalog(self):
        return self.spark.createDataFrame(
            [("V0001", "P1", "van", "Paris", 500), ("V0002", "P2", "truck", "Lyon", 1000)], fs.CATALOG_SCHEMA)

    def test_queries_run_together_and_give_the_expected_results(self):
        self.write_file("f1.json", [
            event("e1", "V0001", 0, 10), event("e1", "V0001", 0, 10),
            event("e2", "V0099", 0, 20, speed=50),
            event("e3", "V0001", 0, 30, speed=130),
            event("e4", "V0001", 0, 40, braking=True),
            "{not json", event("bad1", "V0001", 0, 50, speed=999),
            json.dumps({"event_id": "bad2", "event_time": BASE.format(m=0, s=1)}),
        ] + [event(f"s{i}", "V0002", i, 30, speed=110) for i in range(1, 7)])
        self.write_file("f2.json", [event("late", "V0001", 1, 5, speed=60)])
        self.write_file("f3.json", [event("future", "V0001", 0, 0, speed=60, hour=13)])

        progress_file = self.tmp / "progress.jsonl"
        fs.install_progress_log(self.spark, str(progress_file))
        raw = self.raw_stream()
        events = fs.parse_and_validate_events(raw)
        valid, invalid = events.filter(F.col("reason").isNull()), events.filter(F.col("reason").isNotNull())
        enriched = fs.enrich_with_catalog(fs.deduplicate_events(valid), self.catalog())

        dlq = (invalid.select("reason").writeStream.format("memory").queryName("dlq").outputMode("append")
               .option("checkpointLocation", str(self.tmp / "c_dlq")).trigger(availableNow=True).start())
        events_query = (enriched.writeStream.foreachBatch(fs.process_event_batch)
                        .option("checkpointLocation", str(self.tmp / "c_events")).trigger(availableNow=True).start())
        kpis = (fs.build_region_kpis(enriched).writeStream.format("memory").queryName("kpis").outputMode("update")
                .option("checkpointLocation", str(self.tmp / "c_kpis")).trigger(availableNow=True).start())
        sustained = (fs.build_sustained_speeding_alerts(enriched).writeStream.format("memory").queryName("sustained")
                     .outputMode("append").option("checkpointLocation", str(self.tmp / "c_sus")).trigger(availableNow=True).start())
        for query in (dlq, events_query, kpis, sustained):
            query.awaitTermination(180)
            self.assertIsNone(query.exception())

        reasons = sorted(r.reason for r in self.spark.sql("SELECT reason FROM dlq").collect())
        self.assertEqual(len(reasons), 3, reasons)
        self.assertIn("speed_out_of_range", reasons)

        stored = self.spark.read.parquet(str(self.tmp / "parquet"))
        ids = sorted(r.event_id for r in stored.collect())
        self.assertEqual(len(ids), len(set(ids)), "duplicate event_id in the Parquet output")
        self.assertEqual(stored.filter(F.col("event_id") == "e1").count(), 1)
        self.assertEqual(stored.filter(F.col("event_id") == "e2").first().region, "UNKNOWN")
        self.assertEqual(stored.filter(F.col("event_id") == "e3").first().region, "Paris")
        self.assertEqual({r.event_date.isoformat() for r in stored.select("event_date").distinct().collect()}, {"2026-10-20"})

        point = sorted(a["alert_id"] for a in self.published)
        self.assertEqual(point, ["e3:speeding", "e4:harsh_braking"])

        kpi_rows = self.spark.sql("SELECT * FROM kpis").collect()
        self.assertTrue(any(r.region == "UNKNOWN" for r in kpi_rows))

        alerts = [json.loads(r.value) for r in self.spark.sql("SELECT value FROM sustained").collect()]
        self.assertTrue(alerts, "no sustained_speeding alert")
        self.assertEqual({a["vehicle_id"] for a in alerts}, {"V0002"})

        lines = [json.loads(line) for line in progress_file.read_text().splitlines()]
        self.assertTrue(lines and all("numInputRows" in line and "batchId" in line for line in lines))

        late_in_parquet = stored.filter(F.col("event_id") == "late").count()
        self.assertEqual(late_in_parquet, 1)


if __name__ == "__main__":
    unittest.main()
