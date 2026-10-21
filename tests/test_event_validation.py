import json
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_jobs.fleet_streaming import parse_and_validate_events


class EventValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("FleetValidationTests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            # Vérifier que même en mode strict une mauvaise date va en DLQ.
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_valid_invalid_and_missing_fields_are_all_classified(self):
        event = {
            "event_id": "test-1", "vehicle_id": "V0001",
            "event_time": "2026-10-18T14:00:00.123+00:00",
            "latitude": 48.8, "longitude": 2.3, "speed_kmh": 50.0,
            "fuel_level_pct": 70.0, "harsh_braking": False,
        }
        cases = []

        def add(value, reason):
            cases.append((json.dumps(value), reason))

        add(event, None)
        for speed in [0, 120, 150, 250]:
            add(dict(event, speed_kmh=speed), None)
        # Les inconnus, doublons et événements tardifs ne sont pas des erreurs de validation.
        add(dict(event, vehicle_id="V0041"), None)
        add(event, None)
        add(dict(event, event_time="2026-10-18T13:54:00.123+00:00"), None)
        for speed in [-12, 999, float("nan")]:
            add(dict(event, speed_kmh=speed), "speed_out_of_range")
        add(dict(event, event_time="not-a-timestamp"), "invalid_event_time")
        add(dict(event, event_time="2026-02-30T14:00:00.123+00:00"), "invalid_event_time")
        add(dict(event, vehicle_id=""), "missing_or_null_vehicle_id")
        for field in event:
            missing = dict(event)
            missing.pop(field)
            add(missing, f"missing_or_null_{field}")
            add(dict(event, **{field: None}), f"missing_or_null_{field}")
        cases.extend([
            ('{"vehicle_id":', "invalid_json_or_field_type"),
            (json.dumps(event)[:-7], "invalid_json_or_field_type"),
            (None, "invalid_json_or_field_type"),
        ])
        add(dict(event, speed_kmh="not-a-number"), "invalid_json_or_field_type")

        schema = StructType([
            StructField("source_key", StringType()),
            StructField("raw_value", StringType()),
            StructField("source_topic", StringType()),
            StructField("source_partition", LongType()),
            StructField("source_offset", LongType()),
        ])
        raw = self.spark.createDataFrame([
            ("V0001", payload, "vehicle_events", 0, index)
            for index, (payload, _) in enumerate(cases)
        ], schema)
        classified = parse_and_validate_events(raw).cache()
        try:
            rows = {row.source_offset: row for row in classified.collect()}
            self.assertEqual(len(rows), len(cases), "Aucun message ne doit disparaître")
            for index, (payload, expected_reason) in enumerate(cases):
                with self.subTest(index=index, expected_reason=expected_reason):
                    self.assertEqual(rows[index].reason, expected_reason)
                    self.assertEqual(rows[index].raw_value, payload)
            valid_count = classified.filter(F.col("reason").isNull()).count()
            invalid_count = classified.filter(F.col("reason").isNotNull()).count()
            self.assertEqual(valid_count + invalid_count, len(cases))
        finally:
            classified.unpersist()


if __name__ == "__main__":
    unittest.main()
