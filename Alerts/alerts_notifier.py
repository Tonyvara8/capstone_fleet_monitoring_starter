import json
import os
import signal
from collections import OrderedDict

from confluent_kafka import Consumer, KafkaError, KafkaException


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "alerts"
GROUP_ID = "alerts-notifier"
SEEN_ALERTS_CAPACITY = 10_000


class SeenAlerts:
    """Last alert_ids handled by this instance (bounded)."""

    def __init__(self, capacity=SEEN_ALERTS_CAPACITY):
        self.capacity = capacity
        self._ids = OrderedDict()

    def already_seen(self, alert_id):
        if alert_id is None:
            return False
        if alert_id in self._ids:
            self._ids.move_to_end(alert_id)
            return True
        self._ids[alert_id] = None
        if len(self._ids) > self.capacity:
            self._ids.popitem(last=False)
        return False


def handle_alert(alert: dict):
    # Entrée : dictionnaire obtenu avec json.loads(message.value()).
    # Sortie : affichage de l'alerte ; aucune valeur retournée.
    if not isinstance(alert, dict):
        raise TypeError("Alert must be a JSON object")

    vehicle_id = alert.get("vehicle_id", "unspecified")
    alert_type = alert.get("alert_type", "unknown")
    print(f"Alert: {alert_type} | from: {vehicle_id} | {alert}", flush=True)


def show_partitions(label: str, partitions):
    # Entrées : texte descriptif et liste d'objets TopicPartition.
    # Sortie : par exemple Current assignment: ['alerts[0]', 'alerts[2]'].
    names = []
    for partition in partitions:
        names.append(f"{partition.topic}[{partition.partition}]")
    print(f"{label}: {names}", flush=True)


def on_assign(consumer, partitions):
    # Le client Kafka fournit le Consumer et les nouvelles partitions à ajouter.
    consumer.incremental_assign(partitions)
    current_partitions = consumer.assignment()
    show_partitions("Current assignment", current_partitions)


def on_revoke(consumer, partitions):
    show_partitions("Revoked partitions", partitions)
    consumer.incremental_unassign(partitions)
    current_partitions = consumer.assignment()
    show_partitions("Current assignment", current_partitions)


def on_lost(consumer, partitions):
    # Le notifier a déjà perdu ces partitions : afficher sans faire de commit.
    show_partitions("Lost partitions", partitions)


def main():
    # 1. Transformer SIGTERM en interruption, comme Ctrl+C.
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    # 2. Créer l'outil de lecture Kafka.
    # Le PID distingue les instances lancées dans des processus différents.
    client_id = f"{GROUP_ID}-{os.getpid()}"
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "client.id": client_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "partition.assignment.strategy": "cooperative-sticky",
        # Seulement si aucun offset valide n'est enregistré pour le groupe.
        "auto.offset.reset": "earliest",
    })

    print(f"[{client_id}] Listening to topic {TOPIC}. Ctrl+C to stop.", flush=True)
    seen = SeenAlerts()
    try:
        # 3. Demander le topic et enregistrer les fonctions de réaffectation.
        # On passe les fonctions sans (), pour les appeler plus tard.
        consumer.subscribe(
            [TOPIC],
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_lost=on_lost,
        )

        # 4. Continuer à lire jusqu'à une interruption ou une erreur.
        while True:
            message = consumer.poll(1.0)

            # 5. Rien reçu pendant cette seconde : recommencer la lecture.
            if message is None:
                continue

            # 6. Vérifier les erreurs Kafka avant de lire le contenu.
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                # 7. Convertir les octets JSON en données Python.
                alert = json.loads(message.value())

                # 8. Traiter l'alerte. Une exception empêche le commit suivant.
                if seen.already_seen(alert.get("alert_id")):
                    print(f"[{client_id}] Duplicate alert ignored: {alert.get('alert_id')}", flush=True)
                else:
                    handle_alert(alert)

                # 9. Confirmer la prochaine position à lire après succès.
                # Exemple : message à l'offset 7 -> offset confirmé 8.
                offsets = consumer.commit(message=message, asynchronous=False)
                for partition in offsets or []:
                    if partition.error is not None:
                        raise KafkaException(partition.error)

                print(
                    f"[{client_id}] Committed: {message.topic()}"
                    f"[{message.partition()}], next offset={message.offset() + 1}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"Failed at {message.topic()}[{message.partition()}] "
                    f"offset={message.offset()}: {exc}",
                    flush=True,
                )
                # Arrêter : ne pas confirmer ensuite un message plus loin
                # dans la partition en sautant celui dont le traitement a échoué.
                raise

    # 10. Ctrl+C ou SIGTERM : sortir normalement de la boucle.
    except KeyboardInterrupt:
        print("Stop requested.", flush=True)
    finally:
        # 11. Toujours fermer le consommateur, même après une erreur.
        # Auto-commit désactivé : aucun message non traité n'est confirmé ici.
        consumer.close()
        print(f"[{client_id}] Consumer closed.", flush=True)


if __name__ == "__main__":
    main()
