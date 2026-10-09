"""Pure functions for validating and normalising USGS GeoJSON observations."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import h3


class EventValidationError(ValueError):
    """Raised when a source feature cannot become a seismic observation."""


def _timestamp_to_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, (int, float)):
        raise EventValidationError(f"{field} must be an epoch timestamp in milliseconds")
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def normalise_feature(feature: dict[str, Any], h3_resolution: int) -> dict[str, Any]:
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coordinates = geometry.get("coordinates") or []
    event_id = feature.get("id")

    if not isinstance(event_id, str) or not event_id:
        raise EventValidationError("feature id is required")
    if geometry.get("type") != "Point" or len(coordinates) < 3:
        raise EventValidationError(f"{event_id}: expected Point geometry with longitude, latitude and depth")

    longitude, latitude, depth_km = coordinates[:3]
    if not isinstance(latitude, (int, float)) or not -90 <= latitude <= 90:
        raise EventValidationError(f"{event_id}: latitude is outside valid bounds")
    if not isinstance(longitude, (int, float)) or not -180 <= longitude <= 180:
        raise EventValidationError(f"{event_id}: longitude is outside valid bounds")

    magnitude = properties.get("mag")
    if magnitude is not None and (not isinstance(magnitude, (int, float)) or not -2 <= magnitude <= 10):
        raise EventValidationError(f"{event_id}: magnitude is outside expected bounds")

    source_updated_at = _timestamp_to_utc(properties.get("updated"), "updated")
    event_time = _timestamp_to_utc(properties.get("time"), "time")
    payload = json.dumps(feature, sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    return {
        "event_id": event_id,
        "source_updated_at": source_updated_at,
        "event_time": event_time,
        "magnitude": magnitude,
        "depth_km": depth_km,
        "longitude": longitude,
        "latitude": latitude,
        "place": properties.get("place"),
        "event_status": properties.get("status"),
        "magnitude_type": properties.get("magType"),
        "source_network": properties.get("net"),
        "h3_cell": h3.latlng_to_cell(latitude, longitude, h3_resolution),
        "payload_hash": payload_hash,
        "raw_payload": payload,
    }
