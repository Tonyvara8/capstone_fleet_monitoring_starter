"""Experiment E3: explains why some events of one simulator run are in Kafka but not in the Parquet output"""
import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = ["event_id", "vehicle_id", "event_time", "latitude", "longitude", "speed_kmh", "fuel_level_pct", "harsh_braking"]
LATE_AFTER_SECONDS = 120  # the job deduplicates with a 2-minute watermark


def classify(value, kafka_timestamp_ms):
    """'malformed' (goes to the DLQ), 'late' (older than the dedup watermark) or 'ok'"""
    try:
        event = json.loads(value)
        if any(event.get(field) is None for field in REQUIRED_FIELDS):
            return "malformed"
        if not 0 <= float(event["speed_kmh"]) <= 250:
            return "malformed"
        event_time = datetime.fromisoformat(event["event_time"]).timestamp()
    except (ValueError, TypeError, AttributeError):
        return "malformed"
    return "late" if kafka_timestamp_ms / 1000 - event_time > LATE_AFTER_SECONDS else "ok"


def explain(kafka_records, parquet_ids, run_id):
    """kafka_records: (value, kafka_timestamp_ms) pairs; parquet_ids: event_ids found in Parquet"""
    pattern = re.compile(rf'"event_id": "(V\d+-{re.escape(run_id)}-\d+)"')
    category_by_id = {}
    for value, timestamp in kafka_records:
        match = pattern.search(value)
        if match and match.group(1) not in category_by_id:
            category_by_id[match.group(1)] = classify(value, timestamp)
    in_parquet = {i for i in parquet_ids if f"-{run_id}-" in i}
    missing = [i for i in category_by_id if i not in in_parquet]
    count = lambda ids, kind: sum(1 for i in ids if category_by_id[i] == kind)
    return {
        "run_id": run_id,
        "distinct_events_in_kafka": len(category_by_id),
        "distinct_events_in_parquet": len(in_parquet),
        "missing_from_parquet": len(missing),
        "missing_malformed_dlq": count(missing, "malformed"),
        "missing_late_dropped_by_watermark": count(missing, "late"),
        "missing_unexplained": count(missing, "ok"),
        "late_events_that_still_reached_parquet": count(in_parquet & set(category_by_id), "late"),
        "in_parquet_but_not_in_kafka": len(in_parquet - set(category_by_id)),
    }


def read_kafka(topic, bootstrap_servers):
    import time
    from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"explain-missing-{time.time_ns()}",
                         "enable.auto.commit": False})
    try:
        partitions = list(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        remaining = {p for p, end in ends.items() if end > 0}
        records = []
        while remaining:
            message = consumer.poll(2.0)
            if message is None or message.error():
                continue
            records.append((message.value().decode("utf-8", "replace"), message.timestamp()[1]))
            if message.offset() >= ends[message.partition()] - 1:
                remaining.discard(message.partition())
        return records
    finally:
        consumer.close()


def read_parquet_ids(path):
    import pyarrow.dataset as ds
    table = ds.dataset(str(path), format="parquet", partitioning="hive").to_table(columns=["event_id"])
    return table.column("event_id").to_pylist()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--topic", default="vehicle_events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--events-path", default=str(PROJECT_ROOT / "data" / "parquet" / "events"))
    args = parser.parse_args(argv)
    result = explain(read_kafka(args.topic, args.bootstrap_servers), read_parquet_ids(args.events_path), args.run_id)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())