
import json

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fleet_catalog"

REGIONS = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]
VEHICLE_TYPES = ["van", "truck", "refrigerated_truck", "motorbike"]
MAX_PAYLOADS_KG = [500, 1000, 1500, 3000]


def build_catalog():
    
    catalog = []
    for number in range(1, 41):
        index = number - 1
        vehicle = {
            "vehicle_id": f"V{number:04d}",
            "plate": f"AB-{number:03d}-CD",
            "vehicle_type": VEHICLE_TYPES[index % len(VEHICLE_TYPES)],
            "home_region": REGIONS[index % len(REGIONS)],
            "max_payload_kg": MAX_PAYLOADS_KG[index % len(MAX_PAYLOADS_KG)],
        }
        catalog.append(vehicle)
    return catalog


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "delivery.timeout.ms": 30000,
    })
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(error, message):
        if error is not None:
            stats["failed"] += 1
            print(f"ECHEC de livraison : {error}")
        else:
            stats["delivered"] += 1
            print(f"Livre : {message.key().decode('utf-8')}")

    catalog = build_catalog()
    for vehicle in catalog:
        producer.produce(
            topic=TOPIC,
            key=vehicle["vehicle_id"].encode("utf-8"),
            value=json.dumps(vehicle).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    remaining = producer.flush(35)
    print(
        f"stopped: {stats['delivered']} delivered, {stats['failed']} failed, "
              f"{remaining} still in the local queue")
    if remaining or stats["failed"] or stats["delivered"] != len(catalog):
        raise SystemExit("Le catalogue n'a pas ete livre integralement.")


if __name__ == "__main__":
    main()
