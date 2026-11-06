import json
import os
import tempfile
import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.fleet_streaming import deduplicate_events


class DeduplicationTest(unittest.TestCase):
    def test_duplicate_event_id_is_removed(self):
        spark = (SparkSession.builder.master("local[2]")
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.session.timeZone", "UTC")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")

        # Trois messages : A est envoyé 2 fois et B 1 fois
        messages = [
            {"event_id": "A", "event_time": "2026-10-20T10:00:00"},
            {"event_id": "A", "event_time": "2026-10-20T10:00:00"},
            {"event_id": "B", "event_time": "2026-10-20T10:00:05"},
        ]
        data_folder = tempfile.mkdtemp()
        checkpoint_folder = tempfile.mkdtemp()
        with open(os.path.join(data_folder, "messages.json"), "w") as file:
            file.write("\n".join(json.dumps(message) for message in messages))

        # Lecture du fichier comme s'il s'agissait d'un flux, puis application de la fonction testée.
        stream = spark.readStream.schema("event_id STRING, event_time STRING").json(data_folder)
        stream = stream.withColumn("event_time", F.to_timestamp("event_time"))
        unique = deduplicate_events(stream)

        query = (unique.writeStream.format("memory").queryName("unique_events")
                 .option("checkpointLocation", checkpoint_folder)
                 .outputMode("append").trigger(availableNow=True).start())
        query.awaitTermination()

        result = sorted(row.event_id for row in spark.sql("SELECT event_id FROM unique_events").collect())
        self.assertEqual(result, ["A", "B"])


if __name__ == "__main__":
    unittest.main()