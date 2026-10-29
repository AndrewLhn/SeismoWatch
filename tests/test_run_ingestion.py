"""Orchestration tests for run_ingestion with the I/O boundary mocked.

These cover the behaviours that unit-testing pure helpers cannot: snapshot
before parse, audit row committed around the load transaction, failed loads
marked 'failed', empty polls writing nothing, and all-rejected polls still
quarantining without advancing the watermark.
"""

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from datetime import UTC

import seismic_pipeline_run as pipeline  # noqa: E402


def raw_feature(feature_id="us0001", **prop_overrides):
    properties = {
        "time": 1735689600000,
        "updated": 1735693200000,
        "mag": 5.3,
        "magType": "mb",
        "net": "us",
        "status": "reviewed",
        "place": "Example region",
    }
    properties.update(prop_overrides)
    return {
        "id": feature_id,
        "geometry": {"type": "Point", "coordinates": [142.1, 38.2, 12.4]},
        "properties": properties,
    }


def broken_feature(feature_id="us0002"):
    feature = raw_feature(feature_id)
    feature["geometry"]["coordinates"] = [142.1, 999.0, 12.4]
    return feature


class RunIngestionTests(unittest.TestCase):
    def run_ingestion_with_mocks(self, features, load_error=None, watermark=None):
        """Execute run_ingestion with every I/O wrapper mocked; return the mocks."""
        patches = {
            name: mock.patch.object(pipeline, name)
            for name in (
                "read_watermark",
                "fetch_usgs_payload",
                "write_snapshot",
                "start_ingestion_run",
                "finish_ingestion_run",
                "write_observations",
            )
        }
        mocks = {name: patch.start() for name, patch in patches.items()}
        self.addCleanup(lambda: [patch.stop() for patch in patches.values()])

        mocks["read_watermark"].return_value = watermark
        mocks["fetch_usgs_payload"].return_value = {"features": features}
        mocks["start_ingestion_run"].return_value = 7
        if load_error is not None:
            mocks["write_observations"].side_effect = load_error

        error = None
        try:
            pipeline.run_ingestion("2024-01-01")
        except Exception as caught:  # noqa: BLE001 - asserted by callers
            error = caught
        return mocks, error

    def test_happy_path_snapshots_then_loads_then_closes_the_run(self):
        mocks, error = self.run_ingestion_with_mocks([raw_feature()])
        self.assertIsNone(error)

        key = pipeline.snapshot_object_key("2024-01-01")
        mocks["fetch_usgs_payload"].assert_called_once()
        params = mocks["fetch_usgs_payload"].call_args[0][0]
        self.assertEqual(params["starttime"], "2024-01-01")
        mocks["write_snapshot"].assert_called_once()
        self.assertEqual(mocks["write_snapshot"].call_args[0][1], key)
        mocks["start_ingestion_run"].assert_called_once_with("2024-01-01", key, 1, 0)
        mocks["write_observations"].assert_called_once()
        mocks["finish_ingestion_run"].assert_called_once_with(7, "succeeded")

    def test_empty_poll_writes_nothing_and_is_not_an_error(self):
        mocks, error = self.run_ingestion_with_mocks([])
        self.assertIsNone(error)
        mocks["write_snapshot"].assert_not_called()
        mocks["start_ingestion_run"].assert_not_called()
        mocks["write_observations"].assert_not_called()

    def test_load_failure_marks_the_audit_row_failed_and_propagates(self):
        mocks, error = self.run_ingestion_with_mocks(
            [raw_feature()], load_error=RuntimeError("pg down")
        )
        self.assertIsInstance(error, RuntimeError)
        mocks["start_ingestion_run"].assert_called_once()
        mocks["finish_ingestion_run"].assert_called_once_with(7, "failed")

    def test_all_rejected_poll_quarantines_without_advancing_watermark(self):
        mocks, error = self.run_ingestion_with_mocks([broken_feature()])
        self.assertIsNone(error)
        mocks["start_ingestion_run"].assert_called_once_with("2024-01-01", mock.ANY, 1, 1)
        _, valid, rejected = mocks["write_observations"].call_args[0][1:]
        self.assertEqual(valid, [])
        self.assertEqual(len(rejected), 1)
        mocks["finish_ingestion_run"].assert_called_once_with(7, "succeeded")

    def test_watermarked_poll_uses_updatedafter(self):
        from datetime import datetime

        mocks, error = self.run_ingestion_with_mocks(
            [raw_feature()], watermark=datetime(2023, 12, 31, tzinfo=UTC)
        )
        self.assertIsNone(error)
        params = mocks["fetch_usgs_payload"].call_args[0][0]
        self.assertEqual(params["updatedafter"], "2023-12-31T00:00:00")
        self.assertNotIn("starttime", params)


class MainEntryPointTests(unittest.TestCase):
    def test_backfill_env_override_reaches_the_query(self):
        with (
            mock.patch.dict("os.environ", {pipeline.BACKFILL_DS_ENV: "2024-01-01"}),
            mock.patch.object(pipeline, "run_ingestion", return_value=0) as run,
        ):
            exit_code = pipeline.main(["--ds", "2026-10-28"])
        self.assertEqual(exit_code, 0)
        run.assert_called_once_with("2024-01-01")


if __name__ == "__main__":
    unittest.main()
