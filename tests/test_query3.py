import json
import unittest
from datetime import datetime

from pyspark.sql import SparkSession

from spark_jobs.fleet_streaming import build_sustained_speeding_alerts

SCHEMA = "event_id STRING, vehicle_id STRING, event_time TIMESTAMP, speed_kmh DOUBLE, region STRING"


class Query3Test(unittest.TestCase):
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

    def alerts_for(self, vehicle, speeds):
        # Un événement par minute, à partir de 10:00:30, pour la liste de vitesses donnée.
        rows = [(f"{vehicle}-{i}", vehicle, datetime(2026, 10, 20, 10, i, 30), float(speed), "Paris")
                for i, speed in enumerate(speeds)]
        events = self.spark.createDataFrame(rows, SCHEMA)
        return [json.loads(r.value) for r in build_sustained_speeding_alerts(events).collect()]

    def test_needs_enough_events_and_a_high_mean(self):
        self.assertGreater(len(self.alerts_for("V1", [110] * 5)), 0)   # 5 événements, moyenne 110 : alerte
        self.assertEqual(self.alerts_for("V2", [110] * 4), [])         # seulement 4 événements : pas d'alerte
        self.assertEqual(self.alerts_for("V3", [100] * 5), [])         # moyenne = 100, pas strictement au-dessus
        self.assertEqual(self.alerts_for("V4", [60] * 5), [])          # roule lentement : pas d'alerte

    def test_alert_id_is_stable(self):
        first = sorted(a["alert_id"] for a in self.alerts_for("V1", [110] * 5))
        second = sorted(a["alert_id"] for a in self.alerts_for("V1", [110] * 5))
        self.assertEqual(first, second)
        self.assertTrue(first[0].startswith("sustained_speeding:V1:"))


if __name__ == "__main__":
    unittest.main()