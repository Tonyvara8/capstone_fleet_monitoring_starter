"""Generate a synthetic day of hand-off Parquet files (same layout and schema as the streaming job writes)"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from spark_jobs.batch_common import create_batch_session

REGIONS = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]


def build_events(spark, date, rows, hot_vehicle, unknown_share, duplicate_share, batches, seed):
    base = spark.range(rows).withColumnRenamed("id", "n")
    r_vehicle = F.rand(seed)
    vehicle_no = (F.rand(seed + 1) * 40).cast("int") + 1
    if hot_vehicle:
        vehicle_no = F.when(r_vehicle < 0.4, F.lit(1)).otherwise(vehicle_no)
    if unknown_share > 0:
        unknown_no = (F.rand(seed + 2) * 5).cast("int") + 41
        vehicle_no = F.when(F.rand(seed + 3) < unknown_share, unknown_no).otherwise(vehicle_no)
    events = base.withColumn("vehicle_no", vehicle_no)
    events = events.withColumn("vehicle_id", F.format_string("V%04d", "vehicle_no"))

    anomaly = F.rand(seed + 4) < 0.03
    fast_anomaly = anomaly & (F.rand(seed + 5) < 0.5)
    speed = F.when(fast_anomaly, F.round(F.rand(seed + 6) * 39 + 121, 1)).otherwise(F.round(F.rand(seed + 7) * 85 + 10, 1))
    seconds = (F.rand(seed + 8) * 86399).cast("int")
    region = F.when(F.col("vehicle_no") > 40, F.lit("UNKNOWN")).otherwise(
        F.element_at(F.array(*[F.lit(r) for r in REGIONS]), ((F.col("vehicle_no") - 1) % 5) + 1))
    events = events.select(
        F.format_string("%s-sample-%d", "vehicle_id", "n").alias("event_id"),
        "vehicle_id",
        F.timestamp_seconds(F.unix_timestamp(F.to_timestamp(F.lit(date))) + seconds).alias("event_time"),
        F.round(F.rand(seed + 9) * 9 + 42, 5).alias("latitude"),
        F.round(F.rand(seed + 10) * 12 - 4, 5).alias("longitude"),
        speed.alias("speed_kmh"),
        F.round(F.rand(seed + 11) * 95 + 5, 1).alias("fuel_level_pct"),
        (anomaly & ~fast_anomaly).alias("harsh_braking"),
        region.alias("region"),
    )
    if duplicate_share > 0:
        dup = events.sample(fraction=duplicate_share, seed=seed + 12)
        events = events.unionByName(dup)
    events = events.withColumn("event_date", F.to_date("event_time"))
    events = events.withColumn("batch_id", (F.rand(seed + 13) * batches).cast("int"))
    return events


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--out", required=True)
    parser.add_argument("--hot-vehicle", action="store_true", help="~40%% of rows from V0001")
    parser.add_argument("--unknown-share", type=float, default=0.0, help="share of rows from vehicles absent from the catalog")
    parser.add_argument("--duplicate-share", type=float, default=0.0)
    parser.add_argument("--batches", type=int, default=20, help="number of batch_id directories")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args(argv)

    spark = create_batch_session("GenerateSampleParquet")
    try:
        events = build_events(spark, args.date, args.rows, args.hot_vehicle, args.unknown_share,
                              args.duplicate_share, args.batches, args.seed)
        writer = events.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
        writer.partitionBy("event_date", "batch_id").parquet(args.out)
        print(f"wrote {args.rows} rows for {args.date} to {args.out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
