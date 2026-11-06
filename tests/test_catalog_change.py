"""Documents what happens to a stream-static join when the static side changes while the query runs (report, B.3)"""
import json
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from spark_jobs.fleet_streaming import CATALOG_SCHEMA, enrich_with_catalog


class CatalogChangeTest(unittest.TestCase):
    def test_changing_the_static_side_never_gives_a_silently_stale_result(self):
        spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        tmp = Path(tempfile.mkdtemp())
        (tmp / "in").mkdir()
        catalog_dir = str(tmp / "catalog")

        def write_catalog(region):
            spark.createDataFrame([("V0001", "P", "van", region, 500)], CATALOG_SCHEMA).write.mode("overwrite").parquet(catalog_dir)

        seen = []

        def collect(batch, batch_id):
            seen.extend((batch_id, r.event_id, r.region) for r in batch.collect())

        write_catalog("Paris")
        (tmp / "in" / "1.json").write_text(json.dumps({"event_id": "before", "vehicle_id": "V0001"}) + "\n")
        stream = spark.readStream.schema("event_id STRING, vehicle_id STRING").json(str(tmp / "in"))
        joined = enrich_with_catalog(stream, spark.read.parquet(catalog_dir))
        query = (joined.writeStream.foreachBatch(collect).option("checkpointLocation", str(tmp / "ckpt"))
                 .trigger(processingTime="1 second").start())
        try:
            deadline = time.time() + 60
            while not seen and time.time() < deadline:
                time.sleep(0.5)
            write_catalog("Lyon")
            (tmp / "in" / "2.json").write_text(json.dumps({"event_id": "after", "vehicle_id": "V0001"}) + "\n")
            while len(seen) < 2 and query.exception() is None and time.time() < deadline:
                time.sleep(0.5)
            failure = query.exception()
        finally:
            query.stop()
            spark.stop()
            shutil.rmtree(tmp, ignore_errors=True)
        if failure is not None:
            print(f"\n[info] query failed: {str(failure)[:300]}", flush=True)
        regions = {event_id: region for _batch, event_id, region in seen}
        print(f"\n[info] regions seen by micro-batches: {regions}", flush=True)
        self.assertEqual(regions.get("before"), "Paris")
        self.assertTrue(regions.get("after") == "Lyon" or failure is not None)


if __name__ == "__main__":
    unittest.main()
