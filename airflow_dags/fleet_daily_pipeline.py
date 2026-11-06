"""DAG fleet_daily_pipeline (Part D.1)"""
import json
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

import fleet_common as common

DEFAULT_ARGS = {"owner": "fleet", "retries": 1, "retry_delay": timedelta(minutes=1)}


def check_topics_exist():
    missing = common.missing_topics()
    if missing:
        raise RuntimeError(f"missing Kafka topics: {missing}")
    print(f"all {len(common.REQUIRED_TOPICS)} topics exist")


def publish_report(ds):
    report_file = common.REPORT_DIR / ds / "report.json"
    report = json.loads(report_file.read_text())
    if common.key_already_in_topic("daily_reports", ds):
        print(f"report of {ds} already in daily_reports: nothing to do")
        return
    common.publish_json("daily_reports", ds, {"report_id": f"daily_report:{ds}", **report})
    print(f"report of {ds} published to daily_reports")


def publish_quality_failed_alert(ds):
    verdict = json.loads((common.QUALITY_DIR / ds / "verdict.json").read_text())
    alert = {
        "alert_id": f"data_quality_failed:{ds}",
        "alert_type": "data_quality_failed",
        "date": ds,
        "reasons": verdict["reasons"],
        "rows": verdict["rows"],
        "duplicate_pct": verdict["duplicate_pct"],
        "unknown_region_pct": verdict["unknown_region_pct"],
    }
    common.publish_json("alerts", "fleet", alert)
    print(f"data_quality_failed alert published for {ds}: {verdict['reasons']}")


@dag(
    dag_id="fleet_daily_pipeline",
    description="Daily quality gate, report and publication of the fleet data",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["capstone", "fleet"],
)
def fleet_daily_pipeline():
    wait_for_day_files = PythonSensor(
        task_id="wait_for_day_files",
        python_callable=common.day_files_ready,
        op_kwargs={"ds": "{{ ds }}"},
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        exponential_backoff=False,
    )

    check_topics = PythonOperator(
        task_id="check_topics",
        python_callable=check_topics_exist,
        retries=5,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
    )

    quality_job = BashOperator(
        task_id="quality_job",
        cwd=str(common.PROJECT_ROOT),
        bash_command=(f"{common.SPARK_PYTHON} spark_jobs/quality_job.py --date {{{{ ds }}}} "
                      f"--events-path {common.EVENTS_PATH} --output-dir {common.QUALITY_DIR}"),
    )

    @task.branch(task_id="read_verdict")
    def read_verdict(ds=None):
        verdict = json.loads((common.QUALITY_DIR / ds / "verdict.json").read_text())
        return "run_report_job" if verdict["passed"] else "publish_quality_failed_alert"

    run_report_job = BashOperator(
        task_id="run_report_job",
        cwd=str(common.PROJECT_ROOT),
        bash_command=(f"{common.SPARK_PYTHON} spark_jobs/daily_report_job.py --date {{{{ ds }}}} "
                      f"--events-path {common.EVENTS_PATH} --output-dir {common.REPORT_DIR}"),
    )

    publish_report_task = PythonOperator(
        task_id="publish_report", python_callable=publish_report, op_kwargs={"ds": "{{ ds }}"})

    alert_task = PythonOperator(
        task_id="publish_quality_failed_alert", python_callable=publish_quality_failed_alert, op_kwargs={"ds": "{{ ds }}"})

    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    verdict_branch = read_verdict()
    wait_for_day_files >> check_topics >> quality_job >> verdict_branch
    verdict_branch >> run_report_job >> publish_report_task >> finish
    verdict_branch >> alert_task >> finish


fleet_daily_pipeline()
