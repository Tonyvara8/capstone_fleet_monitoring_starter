from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, from_json, isnan, lit, struct, to_json, trim, try_to_timestamp, when


BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_ROOT = Path(__file__).resolve().parents[1] / "checkpoints" / "validation_v1"

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
    preview = valid_events.select("event_id", "vehicle_id", "event_time", "speed_kmh")

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

        # Démarrer les deux sorties et garder leurs références pour pouvoir les arrêter.
        dlq_query = start_dlq_query(invalid_events)
        queries.append(dlq_query)

        preview_query = start_valid_preview(valid_events)
        queries.append(preview_query)

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
