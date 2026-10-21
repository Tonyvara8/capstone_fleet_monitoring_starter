import json
import unittest

from pyspark.sql import SparkSession

from spark_jobs.fleet_streaming import enrich_with_catalog, parse_catalog

CATALOG_SCHEMA = "vehicle_id STRING, plate STRING, vehicle_type STRING, home_region STRING, max_payload_kg LONG"


class EnrichmentTest(unittest.TestCase):
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

    def test_unknown_vehicle_gets_region_unknown_and_is_not_lost(self):
        events = self.spark.createDataFrame([("e1", "V0001"), ("e2", "V0099")], "event_id STRING, vehicle_id STRING")
        catalog = self.spark.createDataFrame([("V0001", "AB-001-CD", "van", "Paris", 500)], CATALOG_SCHEMA)

        result = {row.vehicle_id: row.region for row in enrich_with_catalog(events, catalog).collect()}

        # V0099 is not in the catalog: it must still be there, with the region UNKNOWN.
        self.assertEqual(result, {"V0001": "Paris", "V0099": "UNKNOWN"})

    def test_catalog_keeps_newest_version_and_ignores_deleted_vehicles(self):
        def record(vehicle_id, region):
            return json.dumps({"vehicle_id": vehicle_id, "plate": "P", "vehicle_type": "van",
                               "home_region": region, "max_payload_kg": 500})

        # Rows as Kafka gives them: key, value, offset.
        raw = self.spark.createDataFrame([
            ("V0001", record("V0001", "Lyon"), 0),
            ("V0001", record("V0001", "Paris"), 5),   # newer version of the same vehicle
            ("V0002", record("V0002", "Lille"), 1),
            ("V0002", None, 6),                       # empty value: V0002 was deleted
        ], "key STRING, value STRING, offset LONG")

        result = {row.vehicle_id: row.home_region for row in parse_catalog(raw).collect()}

        self.assertEqual(result, {"V0001": "Paris"})


if __name__ == "__main__":
    unittest.main()