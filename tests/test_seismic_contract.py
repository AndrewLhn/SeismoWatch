import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import h3  # noqa: E402
from seismic_contract import EventValidationError, normalise_feature  # noqa: E402


def feature(**overrides):
    result = {
        "id": "us7000example",
        "geometry": {"type": "Point", "coordinates": [142.1, 38.2, 12.4]},
        "properties": {
            "time": 1735689600000,
            "updated": 1735693200000,
            "mag": 5.3,
            "magType": "mb",
            "net": "us",
            "status": "reviewed",
            "place": "Example region",
        },
    }
    result.update(overrides)
    return result


class SeismicContractTests(unittest.TestCase):
    def test_normalises_valid_feature_with_stable_payload_hash(self):
        event = normalise_feature(feature(), h3_resolution=5)
        self.assertEqual(event["event_id"], "us7000example")
        self.assertEqual(event["h3_cell"][:1], "8")
        self.assertEqual(len(event["payload_hash"]), 64)

    def test_extracts_depth_from_third_coordinate(self):
        event = normalise_feature(feature(), h3_resolution=5)
        self.assertAlmostEqual(event["depth_km"], 12.4)
        self.assertAlmostEqual(event["longitude"], 142.1)
        self.assertAlmostEqual(event["latitude"], 38.2)

    def test_h3_cell_matches_direct_indexing(self):
        event = normalise_feature(feature(), h3_resolution=5)
        self.assertEqual(event["h3_cell"], h3.latlng_to_cell(38.2, 142.1, 5))

    def test_accepts_missing_magnitude(self):
        props = feature()["properties"] | {"mag": None}
        event = normalise_feature(feature(properties=props), h3_resolution=5)
        self.assertIsNone(event["magnitude"])

    def test_rejects_magnitude_outside_bounds(self):
        props = feature()["properties"] | {"mag": 12.0}
        with self.assertRaises(EventValidationError):
            normalise_feature(feature(properties=props), h3_resolution=5)

    def test_rejects_non_numeric_source_timestamp(self):
        props = feature()["properties"] | {"updated": "yesterday"}
        with self.assertRaises(EventValidationError):
            normalise_feature(feature(properties=props), h3_resolution=5)

    def test_rejects_invalid_coordinate(self):
        invalid = feature(geometry={"type": "Point", "coordinates": [142.1, 100, 12.4]})
        with self.assertRaises(EventValidationError):
            normalise_feature(invalid, h3_resolution=5)

    def test_accepts_antimeridian_longitude(self):
        geometry = {"type": "Point", "coordinates": [180.0, -90.0, 10.0]}
        event = normalise_feature(feature(geometry=geometry), h3_resolution=5)
        self.assertEqual(event["longitude"], 180.0)

    def test_rejects_non_point_geometry(self):
        invalid = feature(geometry={"type": "Polygon", "coordinates": [[[0, 0, 0]]]})
        with self.assertRaises(EventValidationError):
            normalise_feature(invalid, h3_resolution=5)

    def test_rejects_missing_source_identifier(self):
        invalid = feature(id="")
        with self.assertRaises(EventValidationError):
            normalise_feature(invalid, h3_resolution=5)

    def test_payload_hash_is_key_order_independent(self):
        first = feature()
        second = {
            "properties": first["properties"],
            "id": first["id"],
            "geometry": first["geometry"],
        }
        self.assertEqual(
            normalise_feature(first, h3_resolution=5)["payload_hash"],
            normalise_feature(second, h3_resolution=5)["payload_hash"],
        )

    def test_revision_of_same_event_is_a_new_version(self):
        original = normalise_feature(feature(), h3_resolution=5)
        revised_props = feature()["properties"] | {"mag": 5.6, "updated": 1735700000000}
        revision = normalise_feature(feature(properties=revised_props), h3_resolution=5)
        self.assertEqual(revision["event_id"], original["event_id"])
        self.assertNotEqual(revision["source_updated_at"], original["source_updated_at"])
        self.assertNotEqual(revision["payload_hash"], original["payload_hash"])


if __name__ == "__main__":
    unittest.main()
