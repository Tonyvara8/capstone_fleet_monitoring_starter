"""Helpers shared by the two Airflow DAGs: paths, Kafka access, sensor condition"""
import json
import os
import time
import uuid
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("FLEET_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
BOOTSTRAP_SERVERS = os.environ.get("FLEET_BOOTSTRAP_SERVERS", "localhost:9092")
SPARK_PYTHON = os.environ.get("FLEET_SPARK_PYTHON", "python3")

DATA_ROOT = Path(os.environ.get("FLEET_DATA_ROOT", PROJECT_ROOT / "data"))
EVENTS_PATH = DATA_ROOT / "parquet" / "events"
QUALITY_DIR = DATA_ROOT / "quality"
REPORT_DIR = DATA_ROOT / "reports"

REQUIRED_TOPICS = ["vehicle_events", "fleet_catalog", "alerts", "region_kpis_1min", "vehicle_events_dlq", "daily_reports"]
NOTIFIER_GROUP = "alerts-notifier"
ALERTS_TOPIC = "alerts"
SETTLE_SECONDS = 60


def day_files_ready(ds, events_path=None, settle_seconds=SETTLE_SECONDS):
    """Sensor condition: the Parquet files of day `ds` exist and have stopped changing"""
    day_dir = Path(events_path or EVENTS_PATH) / f"event_date={ds}"
    files = [p for p in day_dir.rglob("*.parquet")] if day_dir.exists() else []
    if not files:
        return False
    if any("_temporary" in p.parts for p in day_dir.rglob("*")):
        return False
    newest = max(p.stat().st_mtime for p in files)
    return time.time() - newest >= settle_seconds


def missing_topics(required=REQUIRED_TOPICS):
    from confluent_kafka.admin import AdminClient
    existing = set(AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=10).topics)
    return [topic for topic in required if topic not in existing]


def publish_json(topic, key, payload):
    """Idempotent producer, synchronous: raises if the message was not acknowledged by the broker"""
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True, "delivery.timeout.ms": 30000})
    errors = []
    producer.produce(topic, key=str(key).encode(), value=json.dumps(payload, sort_keys=True).encode(),
                     on_delivery=lambda err, _msg: errors.append(err) if err else None)
    remaining = producer.flush(35)
    if remaining or errors:
        raise RuntimeError(f"publication to {topic} failed: {errors or 'timeout'}")


def key_already_in_topic(topic, key, partition=0):
    """True when a message with this key is already in the (single-partition) topic: makes the publication idempotent"""
    from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP_SERVERS, "group.id": f"idempotence-check-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False})
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        if high <= low:
            return False
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
        deadline = time.time() + 30
        while time.time() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            if message.key() is not None and message.key().decode() == str(key):
                return True
            if message.offset() >= high - 1:
                return False
        raise TimeoutError(f"could not read {topic} up to offset {high}")
    finally:
        consumer.close()


def consumer_lag(group=NOTIFIER_GROUP, topic=ALERTS_TOPIC):
    """{partition: lag} where lag = log end offset - committed offset of `group`"""
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP_SERVERS, "group.id": group, "enable.auto.commit": False})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        committed = consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=10)
        lags = {}
        for tp in committed:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10, cached=False)
            position = tp.offset if tp.offset >= 0 else low
            lags[tp.partition] = max(0, high - position)
        return lags
    finally:
        consumer.close()
