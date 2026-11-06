"""Part E performance study: end-to-end latency of the alerts produced after a given instant"""
import argparse
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))]


def alert_latencies_ms(messages, since_ms):
    latencies = []
    for timestamp_ms, value in messages:
        if timestamp_ms < since_ms:
            continue
        try:
            alert = json.loads(value)
            event_time = datetime.fromisoformat(alert["event_time"].replace("Z", "+00:00"))
        except Exception:
            continue
        latencies.append(timestamp_ms - event_time.timestamp() * 1000)
    return latencies


def read_alerts_with_timestamps(bootstrap):
    from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"measure-{time.time_ns()}", "enable.auto.commit": False})
    try:
        partitions = list(consumer.list_topics("alerts", timeout=10).topics["alerts"].partitions)
        ends = {p: consumer.get_watermark_offsets(TopicPartition("alerts", p), timeout=10)[1] for p in partitions}
        consumer.assign([TopicPartition("alerts", p, OFFSET_BEGINNING) for p in partitions])
        out, done, deadline = [], {p: ends[p] == 0 for p in partitions}, time.time() + 120
        while not all(done.values()) and time.time() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            out.append((message.timestamp()[1], message.value()))
            if message.offset() >= ends[message.partition()] - 1:
                done[message.partition()] = True
        return out
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since-ms", type=int, required=True, help="start of the steady load (printed by load_generator.py)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    latencies = alert_latencies_ms(read_alerts_with_timestamps(args.bootstrap_servers), args.since_ms)
    seconds = lambda ms: round(ms / 1000, 2)
    result = {"alerts_measured": len(latencies),
              "latency_p50_s": seconds(percentile(latencies, 0.5)) if latencies else None,
              "latency_p95_s": seconds(percentile(latencies, 0.95)) if latencies else None,
              "latency_max_s": seconds(max(latencies)) if latencies else None,
              "latency_mean_s": seconds(statistics.mean(latencies)) if latencies else None}
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
