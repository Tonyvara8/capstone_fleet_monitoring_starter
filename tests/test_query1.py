import json
import tempfile
import unittest
from datetime import datetime

from pyspark.sql import SparkSession

from spark_jobs.fleet_streaming import build_point_alerts, write_events_batch

SCHEMA = ("event_id STRING, vehicle_id STRING, event_time TIMESTAMP, latitude DOUBLE, longitude DOUBLE, "
          "speed_kmh DOUBLE, fuel_level_pct DOUBLE, harsh_braking BOOLEAN, region STRING")


def make_row(event_id, speed=50.0, braking=False, minute=0):
    return (event_id, "V0001", datetime(2026, 10, 20, 10, minute, 0), 48.8, 2.3, float(speed), 70.0, braking, "Paris")


class Query1Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_one_alert_per_broken_rule(self):
        events = self.spark.createDataFrame([
            make_row("normal", speed=80),
            make_row("limit", speed=120),                      # exactement 120 : pas au-dessus du seuil
            make_row("fast", speed=121),
            make_row("brake", speed=50, braking=True),
            make_row("both", speed=150, braking=True),         # deux règles : deux alertes
        ], SCHEMA)

        alerts = [json.loads(row.value) for row in build_point_alerts(events).collect()]
        ids = sorted(alert["alert_id"] for alert in alerts)

        self.assertEqual(ids, ["both:harsh_braking", "both:speeding", "brake:harsh_braking", "fast:speeding"])

    def test_replaying_a_batch_does_not_duplicate_rows(self):
        events = self.spark.createDataFrame([make_row("1"), make_row("2"), make_row("3", minute=1)], SCHEMA)
        with tempfile.TemporaryDirectory() as path:
            write_events_batch(events, 7, path)
            write_events_batch(events, 7, path)   # crash puis rejeu du même batch
            self.assertEqual(self.spark.read.parquet(path).count(), 3)
            write_events_batch(events, 8, path)   # un nouveau batch ajoute ses lignes sans rien retirer
            self.assertEqual(self.spark.read.parquet(path).count(), 6)


if __name__ == "__main__":
    unittest.main()