"""Part E: load generator for the failure experiments and the performance study"""
import argparse
import json
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "producers"))

import produce_vehicle_events as sim
from confluent_kafka import Producer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="vehicle_events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=int, default=200, help="events per second (0 = burst)")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--count", type=int, default=0, help="stop after N events (overrides --seconds)")
    parser.add_argument("--hot-vehicle", action="store_true")
    args = parser.parse_args()

    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, _msg):
        stats["failed" if err else "delivered"] += 1

    producer = Producer({"bootstrap.servers": args.bootstrap_servers, "enable.idempotence": True,
                         "linger.ms": 20, "batch.size": 65536, "compression.type": "lz4"})
    vehicle_ids = [f"V{i:04d}" for i in range(1, sim.NUM_VEHICLES + 1)]
    sent, seq, start = 0, 0, time.time()
    print(f"run {sim.RUN_ID}: topic={args.topic} rate={args.rate or 'burst'} count={args.count or '-'} seconds={args.seconds}", flush=True)
    try:
        while True:
            tick = time.time()
            batch = args.rate if args.rate else 5000
            for _ in range(batch):
                vehicle_id = "V0001" if args.hot_vehicle and random.random() < sim.HOT_SHARE else random.choice(vehicle_ids)
                seq += 1
                event = sim.make_event(vehicle_id, seq, False)
                payload = sim.corrupt(event) if random.random() < sim.MALFORMED_RATE else json.dumps(event)
                while True:
                    try:
                        producer.produce(args.topic, key=vehicle_id, value=payload, on_delivery=on_delivery)
                        break
                    except BufferError:
                        producer.poll(0.5)
                sent += 1
                if random.random() < sim.DUPLICATE_RATE:
                    producer.produce(args.topic, key=vehicle_id, value=payload, on_delivery=on_delivery)
                if args.count and sent >= args.count:
                    break
            producer.poll(0)
            if (args.count and sent >= args.count) or (not args.count and time.time() - start >= args.seconds):
                break
            if args.rate:
                time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    except KeyboardInterrupt:
        pass
    finally:
        remaining = producer.flush(60)
        elapsed = time.time() - start
        print(f"sent {sent} events in {elapsed:.1f}s ({sent / elapsed:.0f}/s): {stats['delivered']} delivered, "
              f"{stats['failed']} failed, {remaining} still queued; started_at_ms={int(start * 1000)}", flush=True)


if __name__ == "__main__":
    main()
