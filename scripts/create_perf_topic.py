"""Part E performance study: create a copy of vehicle_events with a chosen number of partitions"""
import argparse

from confluent_kafka.admin import AdminClient, NewTopic

parser = argparse.ArgumentParser()
parser.add_argument("--partitions", type=int, required=True)
parser.add_argument("--name", default="")
parser.add_argument("--bootstrap-servers", default="localhost:9092")
args = parser.parse_args()
name = args.name or f"vehicle_events_p{args.partitions}"
admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
if name in admin.list_topics(timeout=10).topics:
    print(f"{name} already exists")
else:
    for topic, future in admin.create_topics([NewTopic(name, num_partitions=args.partitions, replication_factor=1)]).items():
        future.result()
        print(f"created {topic}")
