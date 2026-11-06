"""DAG kafka_lag_monitor (Part D.2): every 5 minutes, measure the lag of the consumer group alerts-notifier per partition"""
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

import fleet_common as common

DEFAULT_LAG_THRESHOLD = 50


@dag(
    dag_id="kafka_lag_monitor",
    description="Lag of the alerts-notifier group, every 5 minutes",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fleet", "retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["capstone", "fleet"],
)
def kafka_lag_monitor():
    @task
    def measure_lag():
        lags = common.consumer_lag()
        print(f"lag of {common.NOTIFIER_GROUP} on {common.ALERTS_TOPIC}: {lags}")
        return {str(partition): lag for partition, lag in lags.items()}

    @task.branch
    def check_threshold(lags):
        threshold = int(Variable.get("fleet_lag_threshold", default_var=DEFAULT_LAG_THRESHOLD))
        worst = max(lags.values(), default=0)
        print(f"worst partition lag = {worst}, threshold = {threshold}")
        return "publish_lag_alert" if worst > threshold else "lag_ok"

    @task
    def publish_lag_alert(lags, data_interval_end=None):
        threshold = int(Variable.get("fleet_lag_threshold", default_var=DEFAULT_LAG_THRESHOLD))
        late = {p: lag for p, lag in lags.items() if lag > threshold}
        alert_id = f"consumer_lag_high:{data_interval_end:%Y%m%dT%H%M}"
        common.publish_json("alerts", "fleet", {
            "alert_id": alert_id,
            "alert_type": "consumer_lag_high",
            "group": common.NOTIFIER_GROUP,
            "topic": common.ALERTS_TOPIC,
            "threshold": threshold,
            "lag_by_partition": lags,
            "partitions_over_threshold": sorted(late),
        })
        print(f"consumer_lag_high published: {late}")

    lags = measure_lag()
    decision = check_threshold(lags)
    alert = publish_lag_alert(lags)
    ok = EmptyOperator(task_id="lag_ok")
    decision >> [alert, ok]


kafka_lag_monitor()
