import unittest
from datetime import datetime

from pyspark.sql import SparkSession

from spark_jobs.fleet_streaming import build_region_kpis

SCHEMA = ("event_id STRING, vehicle_id STRING, event_time TIMESTAMP, "
          "speed_kmh DOUBLE, region STRING")


class Query2Test(unittest.TestCase):
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

    def test_kpis_per_region_and_minute(self):
        events = self.spark.createDataFrame([
            ("1", "V1", datetime(2026, 10, 20, 10, 0, 10), 100.0, "Paris"),
            ("2", "V2", datetime(2026, 10, 20, 10, 0, 50), 130.0, "Paris"),   # > 120 : excès de vitesse
            ("3", "V3", datetime(2026, 10, 20, 10, 1, 5), 60.0, "Paris"),     # minute suivante : autre fenêtre
            ("4", "V4", datetime(2026, 10, 20, 10, 0, 30), 80.0, "Lyon"),     # autre région : autre ligne
        ], SCHEMA)

        rows = {(r.region, r.window_start.minute): r for r in build_region_kpis(events).collect()}

        paris = rows[("Paris", 0)]
        self.assertEqual(paris.event_count, 2)
        self.assertEqual(paris.mean_speed_kmh, 115.0)
        self.assertEqual(paris.max_speed_kmh, 130.0)
        self.assertEqual(paris.speeding_events, 1)
        self.assertEqual(rows[("Paris", 1)].event_count, 1)
        self.assertEqual(rows[("Lyon", 0)].event_count, 1)
        self.assertEqual(len(rows), 3)


if __name__ == "__main__":
    unittest.main()