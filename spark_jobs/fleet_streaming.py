from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, coalesce, col, concat_ws, from_json, isnan, lit, row_number, struct, to_date, to_json, trim, try_to_timestamp, when

BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_ROOT = Path(__file__).resolve().parents[1] / "checkpoints" / "validation_v1"

DEDUP_WATERMARK = "2 minutes"
CATALOG_SCHEMA = "vehicle_id STRING, plate STRING, vehicle_type STRING, home_region STRING, max_payload_kg LONG"
STREAMING_CHECKPOINT_ROOT = Path(__file__).resolve().parents[1] / "checkpoints" / "streaming_v1"
PARQUET_EVENTS_PATH = Path(__file__).resolve().parents[1] / "data" / "parquet" / "events"
SPEED_ALERT_THRESHOLD = 120  # alerte si la vitesse est strictement supérieure (km/h)
SUSTAINED_SPEED_THRESHOLD = 100  # excès prolongé : vitesse moyenne strictement supérieure (km/h)
SUSTAINED_MIN_EVENTS = 5  # nombre minimum d'événements dans la fenêtre de 5 minutes
STORED_COLUMNS = ["event_id", "vehicle_id", "event_time", "latitude", "longitude",
                  "speed_kmh", "fuel_level_pct", "harsh_braking", "region"]

# Le schéma indique le nom et le type des informations attendues.
# STRING = texte, DOUBLE = nombre décimal, BOOLEAN = vrai ou faux.
# _corrupt_record reçoit le texte si Spark ne peut pas le lire correctement.
EVENT_SCHEMA = """
    event_id STRING,
    vehicle_id STRING,
    event_time STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_kmh DOUBLE,
    fuel_level_pct DOUBLE,
    harsh_braking BOOLEAN,
    _corrupt_record STRING
"""


def create_spark_session():
    spark = SparkSession.builder.appName("FleetMonitoringValidation").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_vehicle_events(spark):
    # Préparer la lecture des messages Kafka.
    reader = spark.readStream.format("kafka")
    reader = reader.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    reader = reader.option("subscribe", "vehicle_events")
    reader = reader.option("startingOffsets", "earliest")
    reader = reader.option("maxOffsetsPerTrigger", "1000")
    reader = reader.option("failOnDataLoss", "true")
    raw = reader.load()

    # Transformer le contenu en texte et donner des noms aux colonnes.
    raw = raw.withColumn("source_key", col("key").cast("string"))
    raw = raw.withColumn("raw_value", col("value").cast("string"))
    raw = raw.withColumnRenamed("topic", "source_topic")
    raw = raw.withColumnRenamed("partition", "source_partition")
    raw = raw.withColumnRenamed("offset", "source_offset")
    raw = raw.withColumnRenamed("timestamp", "source_timestamp")
    return raw.select("source_key", "raw_value", "source_topic", "source_partition", "source_offset", "source_timestamp")


def parse_and_validate_events(raw_events):
    # Lire le texte JSON et placer les informations dans une colonne appelée data.
    options = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
    events = raw_events.withColumn("data", from_json("raw_value", EVENT_SCHEMA, options))

    # Sortir les champs de data pour en faire des colonnes du tableau.
    events = events.select("*", "data.*")
    events = events.drop("data")

    # Garder la date originale, puis créer une colonne contenant la date convertie.
    events = events.withColumnRenamed("event_time", "event_time_raw")
    date_format = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    events = events.withColumn("event_time", try_to_timestamp("event_time_raw", lit(date_format)))

    # Construire les règles : quand une condition est vraie, retenir son motif.
    # | signifie OU ; isNull() signifie « valeur absente ».
    bad_json = col("raw_value").isNull() | col("_corrupt_record").isNotNull()
    reason = when(bad_json, "invalid_json_or_field_type")

    reason = reason.when(col("event_id").isNull() | (trim(col("event_id")) == ""), "missing_or_null_event_id")
    reason = reason.when(col("vehicle_id").isNull() | (trim(col("vehicle_id")) == ""), "missing_or_null_vehicle_id")
    reason = reason.when(col("event_time_raw").isNull() | (trim(col("event_time_raw")) == ""), "missing_or_null_event_time")
    reason = reason.when(col("latitude").isNull(), "missing_or_null_latitude")
    reason = reason.when(col("longitude").isNull(), "missing_or_null_longitude")
    reason = reason.when(col("speed_kmh").isNull(), "missing_or_null_speed_kmh")
    reason = reason.when(col("fuel_level_pct").isNull(), "missing_or_null_fuel_level_pct")
    reason = reason.when(col("harsh_braking").isNull(), "missing_or_null_harsh_braking")
    reason = reason.when(col("event_time").isNull(), "invalid_event_time")

    # isnan détecte une valeur numérique spéciale qui ne représente pas un nombre.
    bad_speed = isnan("speed_kmh") | (col("speed_kmh") < 0) | (col("speed_kmh") > 250)
    reason = reason.when(bad_speed, "speed_out_of_range")

    # Si aucune règle ne s'applique, il n'y a pas d'erreur : reason vaut NULL.
    reason = reason.otherwise(lit(None).cast("string"))
    events = events.withColumn("reason", reason)
    return events

def deduplicate_events(valid_events):
    events = valid_events.withWatermark("event_time", DEDUP_WATERMARK)

    return events.dropDuplicatesWithinWatermark(["event_id"])

def parse_catalog(raw_catalog):
    # Input: Kafka rows with key, value and offset. Output: one row per vehicle.
    catalog = raw_catalog.select(
        col("key").cast("string").alias("vehicle_id"),
        col("value").cast("string").alias("raw_value"),
        col("offset"),
    )

    # A compacted topic can still hold several versions of a key: keep the highest offset.
    newest_first = Window.partitionBy("vehicle_id").orderBy(col("offset").desc())
    catalog = catalog.withColumn("version_rank", row_number().over(newest_first))
    catalog = catalog.filter(col("version_rank") == 1)

    # A newest record with an empty value is a tombstone: the vehicle was removed.
    catalog = catalog.filter(col("raw_value").isNotNull())
    catalog = catalog.withColumn("data", from_json("raw_value", CATALOG_SCHEMA))
    return catalog.select("vehicle_id", "data.plate", "data.vehicle_type", "data.home_region", "data.max_payload_kg")


def read_catalog(spark):
    # Batch read (not a stream): the catalog is small reference data, 40 vehicles.
    reader = spark.read.format("kafka")
    reader = reader.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    reader = reader.option("subscribe", "fleet_catalog")
    reader = reader.option("startingOffsets", "earliest")
    reader = reader.option("endingOffsets", "latest")
    return parse_catalog(reader.load())


def enrich_with_catalog(events, catalog):
    # broadcast() sends the small catalog to every task, so the big stream is not shuffled.
    # LEFT join: an event whose vehicle is not in the catalog is kept (an inner join would drop it).
    enriched = events.join(broadcast(catalog), on="vehicle_id", how="left")

    # Unknown vehicles have no home_region: replace the missing value by UNKNOWN.
    return enriched.withColumn("region", coalesce(col("home_region"), lit("UNKNOWN")))

def to_kafka_messages(rows, key_column):
    # Kafka n'accepte que deux colonnes : key et value.
    # Toutes les autres colonnes sont rangées dans un texte JSON (to_json).
    key = col(key_column).cast("string").alias("key")
    value = to_json(struct(*rows.columns)).alias("value")
    return rows.select(key, value)


def build_point_alerts(batch):
    # Une alerte par règle non respectée : un événement trop rapide ET avec freinage brusque donne deux alertes.
    speeding = batch.filter(col("speed_kmh") > SPEED_ALERT_THRESHOLD)
    speeding = speeding.withColumn("alert_type", lit("speeding"))
    braking = batch.filter(col("harsh_braking"))
    braking = braking.withColumn("alert_type", lit("harsh_braking"))
    alerts = speeding.unionByName(braking)

    # alert_id est construit à partir de l'événement : le même événement donne toujours le même alert_id.
    # Le notifier peut ainsi reconnaître une alerte publiée deux fois après un rejeu.
    alerts = alerts.withColumn("alert_id", concat_ws(":", "event_id", "alert_type"))
    alerts = alerts.select("alert_id", "alert_type", "vehicle_id", "region", "event_id",
                           "event_time", "speed_kmh", "harsh_braking")
    return to_kafka_messages(alerts, "vehicle_id")


def write_events_batch(batch, batch_id, path=PARQUET_EVENTS_PATH):
    # Les dossiers s'appellent event_date=.../batch_id=.../.
    # Si le batch N est rejoué, Spark réécrit exactement les dossiers batch_id=N et ne touche pas aux autres.
    partitioned = batch.withColumn("event_date", to_date("event_time"))
    partitioned = partitioned.withColumn("batch_id", lit(int(batch_id)))

    writer = partitioned.write.mode("overwrite")
    # "dynamic" = remplacer seulement les partitions présentes dans ce lot.
    # Avec la valeur par défaut ("static"), overwrite effacerait tout le jeu de données.
    writer = writer.option("partitionOverwriteMode", "dynamic")
    writer = writer.partitionBy("event_date", "batch_id")
    writer.parquet(str(path))


def publish_alerts(alert_messages):
    # Écriture ordinaire (pas en streaming) de lignes key/value dans le topic alerts.
    writer = alert_messages.write.format("kafka")
    writer = writer.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    writer = writer.option("topic", "alerts")
    writer.save()


def process_event_batch(batch, batch_id):
    # Spark appelle cette fonction une fois par micro-batch, avec un DataFrame ordinaire.
    # Après un crash, Spark la rappelle avec le MÊME batch_id et les MÊMES lignes.
    if batch.isEmpty():
        return

    # Le lot sert deux fois (Parquet puis alertes) : on le garde en mémoire au lieu de le recalculer.
    batch = batch.select(*STORED_COLUMNS).persist()
    try:
        # L'ordre compte. Parquet d'abord : rejouable sans effet (idempotent).
        # Kafka ensuite : non idempotent, donc on le fait en dernier pour réduire la fenêtre où un crash crée un doublon.
        write_events_batch(batch, batch_id)
        publish_alerts(build_point_alerts(batch))
    finally:
        batch.unpersist()


def start_events_query(enriched_events):
    writer = enriched_events.writeStream.foreachBatch(process_event_batch)
    writer = writer.option("checkpointLocation", str(STREAMING_CHECKPOINT_ROOT / "events"))
    writer = writer.trigger(processingTime="5 seconds")
    return writer.start()

def build_region_kpis(enriched_events):
    # Fenêtre fixe (tumbling) : tranches d'1 minute qui ne se chevauchent pas, un événement appartient à une seule tranche.
    # La tranche est choisie avec event_time (quand l'événement a eu lieu), pas avec l'heure d'arrivée dans Spark.
    window = F.window("event_time", "1 minute")
    speeding = when(col("speed_kmh") > SPEED_ALERT_THRESHOLD, 1).otherwise(0)

    kpis = enriched_events.groupBy(window, "region").agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("speed_kmh"), 2).alias("mean_speed_kmh"),
        F.max("speed_kmh").alias("max_speed_kmh"),
        F.sum(speeding).alias("speeding_events"),
    )
    return kpis.select(
        "region",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_count", "mean_speed_kmh", "max_speed_kmh", "speeding_events",
    )


def start_region_kpis_query(enriched_events):
    # Clé = region : tous les indicateurs d'une région vont dans la même partition, dans l'ordre.
    messages = to_kafka_messages(build_region_kpis(enriched_events), "region")

    writer = messages.writeStream.format("kafka")
    writer = writer.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    writer = writer.option("topic", "region_kpis_1min")
    writer = writer.option("checkpointLocation", str(STREAMING_CHECKPOINT_ROOT / "region_kpis"))
    # update = renvoyer une fenêtre à chaque fois que de nouveaux événements la modifient, sans attendre sa fermeture.
    writer = writer.outputMode("update")
    writer = writer.trigger(processingTime="5 seconds")
    return writer.start()

def build_sustained_speeding_alerts(enriched_events):
    # Fenêtre glissante : 5 minutes de long, une nouvelle fenêtre démarre chaque minute.
    # Les fenêtres se chevauchent : un événement est compté dans 5 fenêtres.
    window = F.window("event_time", "5 minutes", "1 minute")

    windowed = enriched_events.groupBy(window, "vehicle_id").agg(
        F.count("*").alias("event_count"),
        F.avg("speed_kmh").alias("mean_speed_kmh"),
        F.first("region").alias("region"),
    )

    # On garde une fenêtre seulement si le véhicule a envoyé assez d'événements ET roulait vite en moyenne.
    sustained = windowed.filter(
        (col("event_count") >= SUSTAINED_MIN_EVENTS) & (col("mean_speed_kmh") > SUSTAINED_SPEED_THRESHOLD)
    )

    # Une alerte par véhicule et par fenêtre ; l'identifiant est stable pour reconnaître les doublons.
    start_text = F.date_format(col("window.start"), "yyyyMMdd'T'HHmmss")
    alerts = sustained.select(
        concat_ws(":", lit("sustained_speeding"), col("vehicle_id"), start_text).alias("alert_id"),
        lit("sustained_speeding").alias("alert_type"),
        "vehicle_id", "region",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_count",
        F.round("mean_speed_kmh", 2).alias("mean_speed_kmh"),
    )
    return to_kafka_messages(alerts, "vehicle_id")


def start_sustained_speeding_query(enriched_events):
    messages = build_sustained_speeding_alerts(enriched_events)

    writer = messages.writeStream.format("kafka")
    writer = writer.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    writer = writer.option("topic", "alerts")
    writer = writer.option("checkpointLocation", str(STREAMING_CHECKPOINT_ROOT / "sustained_speeding"))
    # append = une fenêtre est envoyée une seule fois, quand le watermark la ferme : une alerte par fenêtre,
    # au prix d'un délai (fin de fenêtre + watermark). Avec update, l'alerte serait répétée à chaque déclenchement.
    writer = writer.outputMode("append")
    writer = writer.trigger(processingTime="5 seconds")
    return writer.start()

def start_dlq_query(invalid_events):
    # Créer une clé comme vehicle_events:0:42 pour retrouver le message d'origine.
    message_key = concat_ws(":", "source_topic", "source_partition", "source_offset")

    # Regrouper les informations de l'erreur, puis les transformer en texte JSON.
    content = struct("reason", "raw_value", "source_key", "source_topic", "source_partition", "source_offset", "source_timestamp")
    message_value = to_json(content, {"ignoreNullFields": "false"})
    messages = invalid_events.select(message_key.alias("key"), message_value.alias("value"))

    # Envoyer les erreurs à Kafka.
    writer = messages.writeStream.format("kafka")
    writer = writer.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    writer = writer.option("topic", "vehicle_events_dlq")
    writer = writer.option("checkpointLocation", str(CHECKPOINT_ROOT / "dlq"))
    writer = writer.outputMode("append")
    writer = writer.trigger(processingTime="5 seconds")
    return writer.start()


def start_valid_preview(valid_events):
    # Afficher cinq événements valides par lot. Ils ne sont pas encore sauvegardés.
    preview = valid_events.select("event_id", "vehicle_id", "region", "event_time", "speed_kmh")

    writer = preview.writeStream.format("console")
    writer = writer.option("checkpointLocation", str(CHECKPOINT_ROOT / "preview"))
    writer = writer.option("truncate", "false")
    writer = writer.option("numRows", "5")
    writer = writer.outputMode("append")
    writer = writer.trigger(processingTime="5 seconds")
    return writer.start()


def main():
    spark = create_spark_session()
    queries = []

    try:
        raw_events = read_vehicle_events(spark)
        events = parse_and_validate_events(raw_events)

        # Séparer les lignes sans erreur et celles qui ont une erreur.
        valid_events = events.filter(col("reason").isNull())
        invalid_events = events.filter(col("reason").isNotNull())
        unique_events = deduplicate_events(valid_events)

        # Add the region of each vehicle from the catalog (Part B.3).
        enriched_events = enrich_with_catalog(unique_events, read_catalog(spark))

        # Démarrer les deux sorties et garder leurs références pour pouvoir les arrêter.
        dlq_query = start_dlq_query(invalid_events)
        queries.append(dlq_query)

        preview_query = start_valid_preview(enriched_events)
        queries.append(preview_query)

        # Sauvegarder les événements en Parquet et publier les alertes (Partie B.4).
        events_query = start_events_query(enriched_events)
        queries.append(events_query)

        # Indicateurs par région et par minute (Partie B.5).
        kpis_query = start_region_kpis_query(enriched_events)
        queries.append(kpis_query)

        # Excès de vitesse prolongé sur 5 minutes (Partie B.6).
        sustained_query = start_sustained_speeding_query(enriched_events)
        queries.append(sustained_query)

        print("Reading vehicle_events. Invalid messages go to vehicle_events_dlq.", flush=True)
        print("Valid events are previewed only. Ctrl+C to stop.", flush=True)
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("Stop requested.", flush=True)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
