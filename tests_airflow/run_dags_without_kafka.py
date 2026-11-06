"""Run the two DAGs for real (Airflow scheduler logic, real Spark jobs) with the Kafka calls replaced by an in-memory fake"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
DATA = Path(tempfile.mkdtemp(prefix="fleet_dag_run_"))
os.environ["FLEET_DATA_ROOT"] = str(DATA)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
sys.path.insert(0, str(PROJECT / "airflow_dags"))

import fleet_common as common

PUBLISHED = []
FAKE_LAG = {"0": 3, "1": 2, "2": 4}


def fake_publish_json(topic, key, payload):
    PUBLISHED.append((topic, str(key), payload))


common.missing_topics = lambda required=common.REQUIRED_TOPICS: []
common.publish_json = fake_publish_json
common.key_already_in_topic = lambda topic, key, partition=0: any(t == topic and k == str(key) for t, k, _ in PUBLISHED)
common.consumer_lag = lambda group=common.NOTIFIER_GROUP, topic=common.ALERTS_TOPIC: {int(p): v for p, v in FAKE_LAG.items()}

from airflow.models import DagBag, Variable


def generate(date, *extra):
    cmd = [common.SPARK_PYTHON, "scripts/generate_sample_parquet.py", "--date", date, "--rows", "30000",
           "--out", str(common.EVENTS_PATH), "--batches", "4", *extra]
    subprocess.run(cmd, cwd=PROJECT, check=True, capture_output=True, env={**os.environ, "PYTHONPATH": str(PROJECT)})
    old = time.time() - 7200
    for path in (common.EVENTS_PATH / f"event_date={date}").rglob("*"):
        os.utime(path, (old, old))


def states(dag_run):
    return {ti.task_id: str(ti.state) for ti in dag_run.get_task_instances()}


def md5(path):
    return hashlib.md5(Path(path).read_bytes()).hexdigest()


def check(label, condition):
    print(("PASS " if condition else "FAIL ") + label, flush=True)
    if not condition:
        raise SystemExit(1)


def main():
    bag = DagBag(dag_folder=str(PROJECT / "airflow_dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    daily, lag = bag.get_dag("fleet_daily_pipeline"), bag.get_dag("kafka_lag_monitor")

    generate("2026-11-05", "--unknown-share", "0.01")
    run = daily.test(execution_date=datetime(2026, 11, 5, tzinfo=timezone.utc))
    s = states(run)
    print(json.dumps(s, indent=1))
    check("day 1: finish succeeded", s["finish"] == "success")
    check("day 1: report path ran", s["run_report_job"] == "success" and s["publish_report"] == "success")
    check("day 1: alert path skipped", s["publish_quality_failed_alert"] == "skipped")
    check("day 1: one report published, no alert", [t for t, *_ in PUBLISHED] == ["daily_reports"])

    generate("2026-11-06", "--unknown-share", "0.2")
    run = daily.test(execution_date=datetime(2026, 11, 6, tzinfo=timezone.utc))
    s = states(run)
    print(json.dumps(s, indent=1))
    check("day 2: finish succeeded although one branch was skipped", s["finish"] == "success")
    check("day 2: report path skipped", s["run_report_job"] == "skipped" and s["publish_report"] == "skipped")
    check("day 2: data_quality_failed alert published",
          PUBLISHED[-1][0] == "alerts" and PUBLISHED[-1][2]["alert_type"] == "data_quality_failed")
    print("alert:", json.dumps(PUBLISHED[-1][2]))

    report = common.REPORT_DIR / "2026-11-05" / "report.json"
    verdict = common.QUALITY_DIR / "2026-11-05" / "verdict.json"
    before = (md5(report), md5(verdict), len(PUBLISHED))
    env = {**os.environ}
    for task_id in ("quality_job", "run_report_job"):
        subprocess.run([str(Path(sys.executable).parent / "airflow"), "tasks", "test", "fleet_daily_pipeline", task_id,
                        "2026-11-05"], check=True, capture_output=True, env=env)
    from airflow.operators.python import PythonOperator
    import fleet_daily_pipeline as module
    module.publish_report("2026-11-05")
    after = (md5(report), md5(verdict), len(PUBLISHED))
    check("rerun: verdict.json identical (md5)", before[1] == after[1])
    check("rerun: report.json identical (md5)", before[0] == after[0])
    check("rerun: report not published twice", before[2] == after[2])

    Variable.set("fleet_lag_threshold", "50")
    run = lag.test(execution_date=datetime(2026, 11, 6, 10, 0, tzinfo=timezone.utc))
    s = states(run)
    check("lag 3/2/4 below threshold 50: no alert", s["lag_ok"] == "success" and s["publish_lag_alert"] == "skipped")
    FAKE_LAG.update({"0": 120, "1": 2, "2": 4})
    run = lag.test(execution_date=datetime(2026, 11, 6, 10, 5, tzinfo=timezone.utc))
    s = states(run)
    check("lag 120 on partition 0: consumer_lag_high published", s["publish_lag_alert"] == "success" and s["lag_ok"] == "skipped")
    alert = PUBLISHED[-1][2]
    check("alert names the late partition", alert["alert_type"] == "consumer_lag_high" and alert["partitions_over_threshold"] == ["0"])
    print("alert:", json.dumps(alert))
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
