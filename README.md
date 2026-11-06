# Real-Time Fleet Monitoring Platform (Kafka, Spark, Airflow)

Capstone project, Big Data & Distributed Frameworks (EFREI Paris). 40 delivery vehicles emit telemetry; the platform raises
immediate alerts, publishes regional indicators every minute and produces a daily report only when the day's data passes a quality gate.

```
simulator -> vehicle_events -> Spark Structured Streaming -> alerts -> notifier (group alerts-notifier, 2 instances)
fleet_catalog (compacted) --^        |-> region_kpis_1min
                                     |-> vehicle_events_dlq
                                     '-> Parquet hand-off -> Spark batch (quality, report) -> daily_reports
                                                                  ^ orchestrated and monitored by Airflow (2 DAGs)
```

## 1. Prerequisites
* Docker (Docker Desktop, or Docker Engine; on Windows use WSL2)
* Python 3.10 or 3.11, Java 11 or 17 (`java -version`)
* Airflow is installed in **its own** virtual environment (section 7), not in `requirements.txt`

## 2. Start the platform

```bash
docker compose up -d                              # Kafka broker (KRaft) on localhost:9092 ; wait ~15 s
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt                   # PySpark 3.5.1 (gives spark-submit), confluent-kafka, pyarrow, ...
python scripts/create_topics.py                   # the six topics
python producers/publish_fleet_catalog.py         # Part A.1: 40 vehicles -> compacted topic fleet_catalog
```

Then, one terminal each (all from the project root, with `.venv` activated):

```bash
# 1. telemetry simulator (faults injected on purpose; options: --rate N, --hot-vehicle, --unknown-vehicles)
python producers/produce_vehicle_events.py --rate 20

# 2. alerts notifier, TWO instances (Part A.2): the log shows the partition assignment of each one
python Alerts/alerts_notifier.py
python Alerts/alerts_notifier.py

# 3. the Spark application: four streaming queries (Part B). First start downloads the Kafka connector.
spark-submit --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_jobs/fleet_streaming.py
```

Streaming options: `--max-offsets-per-trigger N` (default 1000), `--trigger-seconds N` (5), `--shuffle-partitions N` (8, stored in the
checkpoints: change it only with fresh checkpoints), `--run-tag T` (isolated checkpoints/output), `--progress-log FILE`
(one JSON line per micro-batch), `--preview` (also print valid events on the console).

Look at a topic:

```bash
docker exec capstone-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic alerts --from-beginning --max-messages 5 --property print.key=true
```
Same command with `vehicle_events_dlq`, `region_kpis_1min`, `daily_reports`.

Stop everything with Ctrl+C. `docker compose down` stops the broker (`-v` erases its data); delete `checkpoints/` and `data/` for a clean start.

## 3. Batch jobs (Part C)

The streaming job writes `data/parquet/events/event_date=YYYY-MM-DD/batch_id=N/`. For one date:

```bash
python spark_jobs/quality_job.py      --date 2026-11-05      # -> data/quality/2026-11-05/verdict.json
python spark_jobs/daily_report_job.py --date 2026-11-05 --explain   # -> data/reports/2026-11-05/report.json ; prints PartitionFilters
python spark_jobs/daily_report_job.py --date 2026-11-05 --salting 8 # same report with two-stage (salted) aggregation
```

Skew study (Part C.3). Real runs with `--hot-vehicle` produce few rows in a few minutes, too few to see task-time differences, so the study
also runs on a synthetic day of 6 million rows with the same schema (V0001 = 40 % of the rows):

```bash
python scripts/generate_sample_parquet.py --date 2026-11-05 --rows 6000000 --hot-vehicle --out data/skew/events
python spark_jobs/skew_study.py --date 2026-11-05 --events-path data/skew/events --output experiments/skew_study.json --keep-ui 600
# Spark UI on http://localhost:4040 while it runs: Stages -> a stage -> "Summary Metrics" (task-time distribution)
```
To run the real hot-vehicle experiment too: start the simulator with `--hot-vehicle` (and a higher `--rate`), then point `--events-path` to `data/parquet/events`.

## 4. Where each part of the subject lives

| Part | Subject item | File / function |
|---|---|---|
| A.1 | catalog, idempotent producer | `producers/publish_fleet_catalog.py` |
| A.2 | notifier, manual commit, cooperative-sticky | `Alerts/alerts_notifier.py` |
| A.3, A.4 | partitions, keys, delivery semantics | report, sections 3 and 4; `scripts/create_topics.py` |
| B.1 | validation + DLQ | `parse_and_validate_events`, `start_dlq_query` |
| B.2 | dedup, 2-minute watermark | `deduplicate_events` |
| B.3 | catalog enrichment, `UNKNOWN` | `parse_catalog`, `read_catalog`, `enrich_with_catalog` |
| B.4 | Query 1 (foreachBatch: Parquet + point alerts) | `process_event_batch`, `write_events_batch`, `build_point_alerts` |
| B.5 | Query 2 (tumbling 1-minute KPIs) | `build_region_kpis`, `start_region_kpis_query` |
| B.6 | Query 3 (sliding 5-minute, sustained speeding) | `build_sustained_speeding_alerts` |
| C.1 | quality job | `spark_jobs/quality_job.py` |
| C.2 | daily report job | `spark_jobs/daily_report_job.py`, `spark_jobs/batch_common.py` |
| C.3 | skew, salting, AQE | `spark_jobs/skew_study.py`, `aggregate_salted`, `join_profile_salted` |
| D.1, D.2 | the two DAGs | `airflow_dags/fleet_daily_pipeline.py`, `airflow_dags/kafka_lag_monitor.py` |
| E | experiments, performance study | `experiments/README.md`, `scripts/count_outputs.py`, `scripts/load_generator.py`, `scripts/run_perf_study.py` |
| E | report | `report/report.pdf` (source `report/report.tex`) |

## 5. Tests

Without Kafka and without Airflow (Java + PySpark only, about 2 minutes):

```bash
export PYTHONPATH=$PWD
python -m unittest discover -s tests -v
```
`tests/test_streaming_pipeline.py` runs the real streaming code on a file stream (Kafka source/sink replaced), with the four queries started together;
the other files test validation, dedup, enrichment, Query 1/2/3, the batch jobs and the notifier. Airflow tests: section 7.

## 6. Topics

| Topic | Partitions | Key | Role |
|---|---|---|---|
| vehicle_events | 6 | vehicle_id | raw telemetry |
| fleet_catalog | 3, compacted | vehicle_id | reference data (you publish it) |
| alerts | 3 | vehicle_id (point alerts), `fleet` (DAG alerts) | alerts from Spark and from the DAGs |
| region_kpis_1min | 3 | region | 1-minute regional indicators |
| vehicle_events_dlq | 3 | `topic:partition:offset` of the source message | invalid messages with a reason |
| daily_reports | 1 | date | daily report published by Airflow |

Telemetry (JSON): `event_id`, `vehicle_id`, `event_time` (ISO 8601 UTC, ms precision), `latitude`, `longitude`, `speed_kmh`, `fuel_level_pct`, `harsh_braking`.
Faults injected by the simulator: ~3 % anomalies, 1 % duplicates, 2 % late events (3-6 min), 1 % corrupted messages, optional hot vehicle and unknown vehicles.

## 7. Airflow (Part D)

```bash
python3.11 -m venv .venv-airflow && source .venv-airflow/bin/activate
pip install "apache-airflow==2.9.3" "confluent-kafka==2.5.0" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt
export AIRFLOW_HOME=$PWD/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow_dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export FLEET_SPARK_PYTHON=$PWD/.venv/bin/python       # the interpreter that has PySpark: the batch jobs run with it
airflow standalone                                    # webserver (http://localhost:8080, password printed at start) + scheduler
```
In another terminal (same environment variables):

```bash
airflow dags unpause fleet_daily_pipeline && airflow dags unpause kafka_lag_monitor
airflow dags trigger fleet_daily_pipeline --exec-date 2026-11-05T00:00:00+00:00     # logical date = the day to process
airflow dags list-runs -d fleet_daily_pipeline
# Prove the tasks are idempotent: run one twice, the output is byte-identical
airflow tasks test fleet_daily_pipeline run_report_job 2026-11-05 && md5sum data/reports/2026-11-05/report.json
airflow tasks test fleet_daily_pipeline run_report_job 2026-11-05 && md5sum data/reports/2026-11-05/report.json
# Backfill the last 7 days (days without files make the sensor wait; days with files are processed)
airflow dags backfill fleet_daily_pipeline -s 2026-10-29 -e 2026-11-04
# Change the lag threshold of the monitor without editing code
airflow variables set fleet_lag_threshold 50
```
The sensor `wait_for_day_files` waits until `data/parquet/events/event_date=<ds>/` has files that have not changed for 60 s. The `daily_reports`
publication is idempotent (the date is the key and an existing key is not published again); alerts carry a stable `alert_id`.

Tests of the DAGs (structure, in the Airflow environment) and a full run of both DAGs with a fake Kafka (real Airflow and Spark):

```bash
python -m unittest discover -s tests_airflow -v
python tests_airflow/run_dags_without_kafka.py
```

## 8. Common problems
* *Cannot connect to localhost:9092*: Docker is not running or the broker is still starting (`docker ps`, wait 15 s).
* *Port 9092 already used*: stop the other Kafka, or change the port in both listener settings of `docker-compose.yml`.
* *Spark cannot find the Kafka source*: add `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` to `spark-submit`.
* *Java not found*: install a JDK 11 or 17 and check `java -version` in the same terminal.
* *`PYTHON_VERSION_MISMATCH` in Spark*: `export PYSPARK_PYTHON=$(which python)` in the environment that runs the jobs.
* *Streaming job refuses to restart after changing `--shuffle-partitions`*: the value is stored in the checkpoints; delete `checkpoints/`.

## 9. Deliverables of the subject (Section 8)
`NAME1_NAME2_capstone.zip` = this project (README, code, tests) + `report/report.pdf` + `experiments/` (raw outputs: logs, Spark UI and Airflow UI screenshots)
+ `CONTRIBUTIONS.md` (one paragraph per student). `scripts/check_submission.py` verifies that everything is there and builds the zip.
