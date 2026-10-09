import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from seismic_contract import EventValidationError, normalise_feature


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

    def test_rejects_invalid_coordinate(self):
        invalid = feature(geometry={"type": "Point", "coordinates": [142.1, 100, 12.4]})
        with self.assertRaises(EventValidationError):
            normalise_feature(invalid, h3_resolution=5)

    def test_rejects_missing_source_identifier(self):
        invalid = feature(id="")
        with self.assertRaises(EventValidationError):
            normalise_feature(invalid, h3_resolution=5)


if __name__ == "__main__":
    unittest.main()
