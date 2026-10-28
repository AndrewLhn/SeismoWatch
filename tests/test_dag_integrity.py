"""DAG structure tests. Skipped when apache-airflow is not installed (they run
in CI and on hosts using requirements-dev.txt)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

try:
    import airflow  # noqa: F401

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


@unittest.skipUnless(AIRFLOW_AVAILABLE, "apache-airflow is not installed")
class DagIntegrityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import seismic_event_pipeline as dag_module

        cls.dag = dag_module.dag

    def test_identity_and_schedule(self):
        self.assertEqual(self.dag.dag_id, "seismic_event_pipeline")
        self.assertEqual(self.dag.timetable.expression, "0 5 * * *")

    def test_task_graph_is_ingest_then_build_then_freshness(self):
        task_ids = [task.task_id for task in self.dag.tasks]
        self.assertEqual(
            task_ids,
            ["ingest_usgs_observations", "build_analytics_models", "check_source_freshness"],
        )
        build = self.dag.get_task("build_analytics_models")
        freshness = self.dag.get_task("check_source_freshness")
        self.assertEqual(build.upstream_task_ids, {"ingest_usgs_observations"})
        self.assertEqual(freshness.upstream_task_ids, {"build_analytics_models"})

    def test_operational_defaults(self):
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.default_args["retries"], 3)
        self.assertIn("execution_timeout", self.dag.default_args)

    def test_ingest_task_runs_the_airflow_free_module(self):
        command = self.dag.get_task("ingest_usgs_observations").bash_command
        self.assertIn("seismic_pipeline_run.py", command)
        self.assertIn("--ds", command)

    def test_dbt_tasks_use_the_isolated_venv(self):
        for task_id in ("build_analytics_models", "check_source_freshness"):
            command = self.dag.get_task(task_id).bash_command
            self.assertIn("/home/airflow/dbt_env/bin/dbt", command)

    def test_freshness_gate_is_a_separate_task(self):
        command = self.dag.get_task("check_source_freshness").bash_command
        self.assertIn("source freshness", command)


if __name__ == "__main__":
    unittest.main()
