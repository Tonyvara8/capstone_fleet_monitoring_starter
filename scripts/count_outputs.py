"""Part E, experiment 1: snapshot of what the platform has produced, to compare before and after a failure"""
import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read_topic(topic, bootstrap, idle_seconds=5):
    """All (partition, offset, key, value) currently in a topic, read without joining any group"""
    from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"count-outputs-{time.time_ns()}", "enable.auto.commit": False})
    try:
        partitions = list(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        ends = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            ends[p] = high
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        rows, reached, last = [], {p: ends[p] == 0 for p in partitions}, time.time()
        while not all(reached.values()) and time.time() - last < idle_seconds + 25:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            last = time.time()
            rows.append((message.partition(), message.offset(), message.key(), message.value()))
            if message.offset() >= ends[message.partition()] - 1:
                reached[message.partition()] = True
        return rows
    finally:
        consumer.close()


def snapshot(args):
    result = {"label": args.label, "taken_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}

    import pyarrow.dataset as ds
    events_path = Path(args.events_path)
    if events_path.exists():
        table = ds.dataset(str(events_path), format="parquet", partitioning="hive").to_table(columns=["event_id"])
        ids = table.column("event_id").to_pylist()
        result["parquet_rows"] = len(ids)
        result["parquet_distinct_event_ids"] = len(set(ids))
        result["parquet_duplicate_rows"] = len(ids) - len(set(ids))
    else:
        result["parquet_rows"] = 0

    alerts = read_topic("alerts", args.bootstrap_servers)
    alert_ids = Counter()
    for _p, _o, _k, value in alerts:
        try:
            alert_ids[json.loads(value).get("alert_id")] += 1
        except Exception:
            alert_ids["<unparsable>"] += 1
    result["alerts_messages"] = len(alerts)
    result["alerts_distinct_alert_ids"] = len(alert_ids)
    result["alerts_duplicated_messages"] = sum(c - 1 for c in alert_ids.values() if c > 1)
    result["dlq_messages"] = len(read_topic("vehicle_events_dlq", args.bootstrap_servers))
    return result


def diff(before_file, after_file):
    before, after = json.loads(Path(before_file).read_text()), json.loads(Path(after_file).read_text())
    for key in sorted(set(before) | set(after)):
        if key in ("label", "taken_at"):
            continue
        print(f"{key:34s} {before.get(key)!s:>12} -> {after.get(key)!s:>12}   (+{(after.get(key) or 0) - (before.get(key) or 0)})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="snapshot")
    parser.add_argument("--events-path", default=str(PROJECT_ROOT / "data" / "parquet" / "events"))
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "experiments"))
    parser.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"))
    args = parser.parse_args()
    if args.diff:
        diff(*args.diff)
        return
    result = snapshot(args)
    out = Path(args.output_dir) / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    print(f"written to {out}")


if __name__ == "__main__":
    sys.exit(main())
