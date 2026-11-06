# Evidence produced while developing, WITHOUT Kafka

These files were produced in a sandbox that has no Docker and therefore no Kafka broker. They show that the code works where it can be
tested without a broker. **They are not the experiments of Part E**, which need the real stack, and must not be presented as such.

| File | What it is |
|---|---|
| `unit_tests_spark.log`, `unit_tests_airflow.log` | the 22 Spark/notifier tests and the 4 DAG structure tests |
| `airflow/run_dags_without_kafka.log` | both DAGs run by Airflow with real Spark jobs and a FAKE Kafka: branches, trigger rule, idempotence, lag alert |
| `skew_study.json`, `skew_study.log`, `spark_ui/stage*.png` | skew study on 6 million SYNTHETIC rows (V0001 = 41 %), `local[2]`, 3 repeats per configuration |
| `spark_ui/sql_report_job_partition_pruning.png`, `report_job/daily_report_explain.log` | partition pruning of the report job on a synthetic two-day dataset (partitions read: 20 of 40) |

Re-run them yourself on your machine (README sections 3, 5 and 7) and put YOUR screenshots in `../spark_ui/` and `../airflow_ui/`.
