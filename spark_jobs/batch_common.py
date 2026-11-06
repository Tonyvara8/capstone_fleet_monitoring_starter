"""Shared helpers for the Spark batch jobs (Part C): hand-off Parquet reading and per-vehicle aggregation"""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVENTS_PATH = PROJECT_ROOT / "data" / "parquet" / "events"
DEFAULT_QUALITY_DIR = PROJECT_ROOT / "data" / "quality"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "reports"

SPEED_ALERT_THRESHOLD = 120
WORST_VEHICLES = 5


def create_batch_session(app_name, shuffle_partitions=8, master="local[*]", aqe=True, extra_conf=None):
    builder = SparkSession.builder.appName(app_name).master(master)
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    builder = builder.config("spark.sql.adaptive.enabled", "true" if aqe else "false")
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_day(spark, events_path, date):
    events = spark.read.parquet(str(events_path))
    return events.filter(F.col("event_date") == F.to_date(F.lit(date)))


def scanned_directories(events):
    """The event_date directories Spark really opens for this DataFrame (not events.inputFiles(), which lists every file"""
    names = events.select(F.input_file_name().alias("f")).distinct().collect()
    return sorted({row.f.split("/event_date=")[1].split("/")[0] for row in names})


def aggregate_plain(events, key):
    """One-stage aggregation per key (vehicle_id or region)"""
    speeding = F.when(F.col("speed_kmh") > SPEED_ALERT_THRESHOLD, 1).otherwise(0)
    braking = F.when(F.col("harsh_braking"), 1).otherwise(0)
    return events.groupBy(key).agg(
        F.count("*").alias("events"),
        F.round(F.avg("speed_kmh"), 2).alias("mean_speed_kmh"),
        F.max("speed_kmh").alias("max_speed_kmh"),
        F.sum(speeding).alias("speeding_events"),
        F.sum(braking).alias("harsh_braking_events"),
    )


def aggregate_salted(events, key, salt_buckets):
    """Two-stage aggregation (salting)"""
    speeding = F.when(F.col("speed_kmh") > SPEED_ALERT_THRESHOLD, 1).otherwise(0)
    braking = F.when(F.col("harsh_braking"), 1).otherwise(0)
    salted = events.withColumn("salt", (F.rand() * salt_buckets).cast("int"))
    partial = salted.groupBy(key, "salt").agg(
        F.count("*").alias("n"),
        F.sum("speed_kmh").alias("speed_sum"),
        F.max("speed_kmh").alias("max_speed_kmh"),
        F.sum(speeding).alias("speeding_events"),
        F.sum(braking).alias("harsh_braking_events"),
    )
    final = partial.groupBy(key).agg(
        F.sum("n").alias("events"),
        F.round(F.sum("speed_sum") / F.sum("n"), 2).alias("mean_speed_kmh"),
        F.max("max_speed_kmh").alias("max_speed_kmh"),
        F.sum("speeding_events").alias("speeding_events"),
        F.sum("harsh_braking_events").alias("harsh_braking_events"),
    )
    return final.select(key, "events", "mean_speed_kmh", "max_speed_kmh", "speeding_events", "harsh_braking_events")


def worst_vehicles(per_vehicle, n=WORST_VEHICLES):
    """The n riskiest vehicles: most speeding + harsh-braking events (ties: higher max speed, then id)"""
    risk = F.col("speeding_events") + F.col("harsh_braking_events")
    ranked = per_vehicle.withColumn("risk_events", risk)
    ranked = ranked.orderBy(F.col("risk_events").desc(), F.col("max_speed_kmh").desc(), F.col("vehicle_id"))
    return ranked.limit(n)


def vehicle_profile(spark):
    """Per-vehicle reference table (plate, type, home region): the 40 catalog vehicles, same generator as the catalog publisher"""
    from producers.publish_fleet_catalog import build_catalog
    return spark.createDataFrame(build_catalog()).select("vehicle_id", "plate", "vehicle_type", "home_region")


def join_profile(events, profile):
    """Plain equi-join on vehicle_id. With a small profile Spark broadcasts it (no shuffle, no skew); the skew study"""
    return events.join(profile, "vehicle_id", "left")


def join_profile_salted(events, profile, salt_buckets):
    """Salted join: the big side gets a random salt in [0, N), the small side is replicated N times (one copy per salt)"""
    salted_events = events.withColumn("salt", (F.rand() * salt_buckets).cast("int"))
    salts = F.explode(F.array(*[F.lit(i) for i in range(salt_buckets)])).alias("salt")
    replicated = profile.select("*", salts)
    return salted_events.join(replicated, ["vehicle_id", "salt"], "left").drop("salt")
