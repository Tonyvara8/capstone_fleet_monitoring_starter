"""Structure tests of the two DAGs. Run inside the Airflow virtual environment:"""
import os
import sys
import unittest
from datetime import timedelta
from pathlib import Path

DAGS = Path(__file__).resolve().parents[1] / "airflow_dags"
sys.path.insert(0, str(DAGS))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

import fleet_common


class DagStructureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(DAGS), include_examples=False)

    def test_no_import_error(self):
        self.assertEqual(self.bag.import_errors, {})
        self.assertEqual(sorted(self.bag.dag_ids), ["fleet_daily_pipeline", "kafka_lag_monitor"])

    def test_daily_pipeline_shape(self):
        dag = self.bag.dags["fleet_daily_pipeline"]
        self.assertEqual(dag.schedule_interval, "@daily")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(sorted(dag.task_ids), sorted([
            "wait_for_day_files", "check_topics", "quality_job", "read_verdict", "run_report_job",
            "publish_report", "publish_quality_failed_alert", "finish"]))

        sensor = dag.get_task("wait_for_day_files")
        self.assertIsInstance(sensor, PythonSensor)
        self.assertEqual(sensor.mode, "reschedule")

        check = dag.get_task("check_topics")
        self.assertGreaterEqual(check.retries, 3)
        self.assertGreater(check.retry_delay, timedelta(0))

        self.assertIsInstance(dag.get_task("read_verdict"), BranchPythonOperator)
        self.assertEqual(sorted(dag.get_task("read_verdict").downstream_task_ids),
                         ["publish_quality_failed_alert", "run_report_job"])

        finish = dag.get_task("finish")
        self.assertIsInstance(finish, EmptyOperator)
        self.assertEqual(finish.trigger_rule, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        self.assertEqual(sorted(finish.upstream_task_ids), ["publish_quality_failed_alert", "publish_report"])

    def test_lag_monitor_shape(self):
        dag = self.bag.dags["kafka_lag_monitor"]
        self.assertEqual(dag.schedule_interval, "*/5 * * * *")
        self.assertFalse(dag.catchup)
        self.assertEqual(sorted(dag.task_ids), ["check_threshold", "lag_ok", "measure_lag", "publish_lag_alert"])
        self.assertEqual(sorted(dag.get_task("check_threshold").downstream_task_ids), ["lag_ok", "publish_lag_alert"])


class SensorConditionTest(unittest.TestCase):
    def test_day_files_ready(self):
        import tempfile
        import time
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(fleet_common.day_files_ready("2026-11-05", tmp, settle_seconds=0))
            part = Path(tmp) / "event_date=2026-11-05" / "batch_id=0" / "part-0.parquet"
            part.parent.mkdir(parents=True)
            part.write_bytes(b"x")
            self.assertFalse(fleet_common.day_files_ready("2026-11-05", tmp, settle_seconds=3600))
            os.utime(part, (time.time() - 7200, time.time() - 7200))
            self.assertTrue(fleet_common.day_files_ready("2026-11-05", tmp, settle_seconds=3600))
            self.assertFalse(fleet_common.day_files_ready("2026-11-06", tmp, settle_seconds=0))


if __name__ == "__main__":
    unittest.main()
