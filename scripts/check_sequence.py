"""Experiment E3: loss, duplicate and ordering check for one simulator run, from the sequence number in event_id"""
import argparse
import json
import re
import sys
import uuid


def analyze(records, run_id):
    pattern = re.compile(rf'"event_id": "(V\d+)-{re.escape(run_id)}-(\d+)"')
    parsed = []
    for partition, offset, key, value in records:
        match = pattern.search(value)
        if match:
            parsed.append((partition, offset, key, int(match.group(2))))
    parsed.sort()

    seqs = [seq for _, _, _, seq in parsed]
    distinct = set(seqs)
    out_of_order = 0
    last_by_key = {}
    for _, _, key, seq in parsed:
        if seq < last_by_key.get(key, 0):
            out_of_order += 1
        last_by_key[key] = max(seq, last_by_key.get(key, 0))

    missing_ranges = []
    if distinct:
        first, last = min(distinct), max(distinct)
        start = None
        for n in range(first, last + 2):
            absent = n <= last and n not in distinct
            if absent and start is None:
                start = n
            if not absent and start is not None:
                missing_ranges.append((start, n - 1))
                start = None
    else:
        first = last = None

    return {
        "run_id": run_id,
        "messages_in_topic": len(seqs),
        "distinct_sequence_numbers": len(distinct),
        "duplicated_messages": len(seqs) - len(distinct),
        "first_sequence": first,
        "last_sequence": last,
        "missing_sequence_numbers": sum(b - a + 1 for a, b in missing_ranges),
        "missing_ranges": missing_ranges[:20],
        "out_of_order_messages": out_of_order,
    }


def read_topic(topic, bootstrap_servers):
    from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"check-seq-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        partitions = list(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        ends = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            ends[p] = (low, high)
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        remaining = {p for p, (low, high) in ends.items() if high > low}
        records = []
        while remaining:
            message = consumer.poll(2.0)
            if message is None:
                continue
            if message.error():
                continue
            partition = message.partition()
            key = message.key().decode() if message.key() else ""
            records.append((partition, message.offset(), key, message.value().decode("utf-8", "replace")))
            if message.offset() >= ends[partition][1] - 1:
                remaining.discard(partition)
        return records
    finally:
        consumer.close()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True, help="8-character run id printed by the simulator, also inside every event_id")
    parser.add_argument("--topic", default="vehicle_events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args(argv)
    result = analyze(read_topic(args.topic, args.bootstrap_servers), args.run_id)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())