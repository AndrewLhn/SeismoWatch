import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from seismic_contract import normalise_feature  # noqa: E402
from seismic_pipeline_run import (  # noqa: E402
    OBSERVATION_COLUMNS,
    effective_logical_date,
    latest_source_updated_at,
    observation_rows,
    partition_features,
    raw_object_uri,
    snapshot_object_key,
    usgs_params,
)


def raw_feature(feature_id="us7000example", **prop_overrides):
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


class LogicalDateTests(unittest.TestCase):
    def test_env_override_wins_for_backfills(self):
        self.assertEqual(effective_logical_date("2024-01-01", "2026-10-28"), "2024-01-01")

    def test_scheduler_ds_is_used_when_no_override(self):
        self.assertEqual(effective_logical_date(None, "2026-10-28"), "2026-10-28")


class QueryParamTests(unittest.TestCase):
    def test_first_run_polls_the_logical_date_window(self):
        params = usgs_params(None, "2024-01-01")
        self.assertEqual(params["starttime"], "2024-01-01")
        self.assertEqual(params["endtime"], "2024-01-02")
        self.assertNotIn("updatedafter", params)
        self.assertEqual(params["format"], "geojson")

    def test_watermarked_run_polls_updates_without_event_time_bound(self):
        # Regression: USGS revises events outside the current logical date, so
        # an incremental poll must not constrain starttime/endtime.
        watermark = datetime(2026, 10, 27, 5, 0, tzinfo=UTC)
        params = usgs_params(watermark, "2026-10-28")
        self.assertEqual(params["updatedafter"], "2026-10-27T05:00:00")
        self.assertNotIn("starttime", params)
        self.assertNotIn("endtime", params)

    def test_naive_watermark_is_read_as_utc(self):
        params = usgs_params(datetime(2026, 10, 27, 5, 0), "2026-10-28")
        self.assertEqual(params["updatedafter"], "2026-10-27T05:00:00")


class SnapshotKeyTests(unittest.TestCase):
    def test_key_is_scoped_to_the_logical_date(self):
        self.assertEqual(
            snapshot_object_key("2024-01-01"),
            "usgs_earthquakes/ingestion_date=2024-01-01/events.json",
        )

    def test_replay_targets_the_same_object(self):
        # ADR-002: replays overwrite the logical date's snapshot instead of
        # accumulating near-duplicate objects per attempt.
        self.assertEqual(snapshot_object_key("2024-01-01"), snapshot_object_key("2024-01-01"))

    def test_uri_is_bucket_qualified(self):
        self.assertEqual(
            raw_object_uri(snapshot_object_key("2024-01-01"), "seismic-raw"),
            "s3://seismic-raw/usgs_earthquakes/ingestion_date=2024-01-01/events.json",
        )


class PartitionTests(unittest.TestCase):
    def test_invalid_features_are_quarantined_not_dropped(self):
        good = raw_feature("us0001")
        bad = raw_feature("us0002")
        bad["geometry"]["coordinates"] = [142.1, 999.0, 12.4]
        valid, rejected = partition_features([good, bad], h3_resolution=5)
        self.assertEqual([event["event_id"] for event in valid], ["us0001"])
        self.assertEqual(rejected[0][0], "us0002")
        self.assertIn("latitude", rejected[0][1])

    def test_feature_without_id_is_quarantined_as_unknown(self):
        bad = raw_feature()
        del bad["id"]
        valid, rejected = partition_features([bad], h3_resolution=5)
        self.assertEqual(valid, [])
        self.assertEqual(rejected[0][0], "unknown")


class RowMappingTests(unittest.TestCase):
    def test_rows_match_the_insert_column_order(self):
        event = normalise_feature(raw_feature(), h3_resolution=5)
        rows = observation_rows([event], "key.json", bucket="seismic-raw")
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), len(OBSERVATION_COLUMNS))
        row = dict(zip(OBSERVATION_COLUMNS, rows[0], strict=True))
        self.assertEqual(row["event_id"], event["event_id"])
        self.assertEqual(row["source_updated_at"], event["source_updated_at"])
        self.assertEqual(row["raw_payload"], event["raw_payload"])
        self.assertEqual(row["raw_object_uri"], "s3://seismic-raw/key.json")

    def test_watermark_is_the_newest_source_update(self):
        older = normalise_feature(raw_feature("us0001", updated=1735693200000), 5)
        newer = normalise_feature(raw_feature("us0002", updated=1735700000000), 5)
        self.assertEqual(latest_source_updated_at([older, newer]), newer["source_updated_at"])

    def test_watermark_stays_put_without_valid_rows(self):
        self.assertIsNone(latest_source_updated_at([]))


if __name__ == "__main__":
    unittest.main()
