#!/usr/bin/env python
"""Fetch, snapshot, and load versioned USGS seismic-event observations.

Runs inside the Airflow image (invoked by a BashOperator) or on any host with
requirements installed. Deliberately imports no Airflow modules: the DAG is a
thin shell around this script, the same split Ledgerline uses, so the whole
ingestion path is unit-testable without an orchestrator.

Flow per run:
    1. Resolve the logical date (env override wins; see ADR-002).
    2. Read the source-update watermark and build the FDSN query.
    3. Snapshot the raw GeoJSON payload to the object store before parsing.
    4. Partition features into valid observations and quarantine rows.
    5. Commit a run-audit row first, then load observations, quarantine, and
       the new watermark in one transaction, and close the audit row.

An empty poll is a valid outcome (no events in the window) and exits clean
without an audit row; a failed poll raises and fails the task.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any

from seismic_contract import EventValidationError, normalise_feature

DAG_ID = "seismic_event_pipeline"
BUCKET = os.getenv("SEISMIC_BUCKET", "seismic-raw")
USGS_BASE_URL = os.getenv("USGS_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
H3_RESOLUTION = int(os.getenv("SEISMIC_H3_RESOLUTION", "5"))
BACKFILL_DS_ENV = "SEISMIC_BACKFILL_DS"

# Insert column order for raw.seismic_event_observations. Single source of
# truth shared by the INSERT statement and the row mapper, so the SQL and the
# tuple layout cannot drift apart silently.
OBSERVATION_COLUMNS = (
    "event_id",
    "source_updated_at",
    "event_time",
    "magnitude",
    "depth_km",
    "longitude",
    "latitude",
    "place",
    "event_status",
    "magnitude_type",
    "source_network",
    "h3_cell",
    "payload_hash",
    "raw_payload",
    "raw_object_uri",
)

_INSERT_OBSERVATIONS = (
    "INSERT INTO raw.seismic_event_observations ("
    + ", ".join(OBSERVATION_COLUMNS)
    + ") VALUES %s ON CONFLICT (event_id, source_updated_at) DO NOTHING"
)


# --- pure decision logic (unit-tested without Airflow, boto3, or psycopg2) ---


def effective_logical_date(env_override: str | None, ds: str) -> str:
    """Pick the logical date this run ingests.

    The environment override exists so `make backfill` and the E2E script can
    replay a fixed historical window through the real DAG without touching
    scheduler state. In normal operation it is unset and the scheduler's ds wins.
    """
    return env_override or ds


def usgs_params(watermark: datetime | None, ds: str) -> dict[str, str]:
    """Build the FDSN query parameters for this run.

    Without a watermark (first run) the poll is bounded by the logical date's
    event-time window. With a watermark the poll asks for everything the source
    revised since the newest observation loaded so far, deliberately without an
    event-time bound: USGS revises older events, and constraining by starttime/
    endtime would silently drop those revisions.
    """
    params = {"format": "geojson", "orderby": "time-asc"}
    if watermark is None:
        start = datetime.strptime(ds, "%Y-%m-%d")
        params["starttime"] = start.strftime("%Y-%m-%d")
        params["endtime"] = (start + timedelta(days=1)).strftime("%Y-%m-%d")
        return params
    if watermark.tzinfo is None:
        # psycopg2 returns timestamptz as aware datetimes; a naive value can
        # only come from a misconfigured environment, so read it as UTC.
        watermark = watermark.replace(tzinfo=UTC)
    params["updatedafter"] = watermark.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return params


def snapshot_object_key(ds: str) -> str:
    """Raw snapshot location, keyed by logical date only.

    A replay of the same date overwrites the same object instead of
    accumulating near-duplicates (ADR-002). Attempt history lives in
    ops.ingestion_runs, not in object names.
    """
    return f"usgs_earthquakes/ingestion_date={ds}/events.json"


def raw_object_uri(object_key: str, bucket: str = BUCKET) -> str:
    return f"s3://{bucket}/{object_key}"


def partition_features(
    features: list[dict[str, Any]], h3_resolution: int
) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """Split raw features into normalised observations and quarantine rows.

    A rejected feature never discards its neighbours: quarantine rows keep the
    source id and the validation reason for ops.rejected_events.
    """
    valid: list[dict[str, Any]] = []
    rejected: list[tuple[str, str]] = []
    for feature in features:
        try:
            valid.append(normalise_feature(feature, h3_resolution))
        except EventValidationError as error:
            rejected.append((str(feature.get("id", "unknown")), str(error)))
    return valid, rejected


def observation_rows(
    events: list[dict[str, Any]], object_key: str, bucket: str = BUCKET
) -> list[tuple]:
    """Map normalised events to insert tuples in OBSERVATION_COLUMNS order."""
    uri = raw_object_uri(object_key, bucket)
    return [
        tuple(event[column] for column in OBSERVATION_COLUMNS[:-1]) + (uri,)
        for event in events
    ]


def latest_source_updated_at(events: list[dict[str, Any]]) -> datetime | None:
    """New watermark value: the newest source publication time loaded."""
    if not events:
        return None
    return max(event["source_updated_at"] for event in events)


# --- I/O wrappers (heavy clients imported lazily so the module stays light) ---


def pg_connection():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )


def read_watermark() -> datetime | None:
    with closing(pg_connection()) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT last_source_updated_at FROM ops.ingestion_watermarks WHERE pipeline_name = %s",
            (DAG_ID,),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def fetch_usgs_payload(params: dict[str, str]) -> dict[str, Any]:
    import requests

    response = requests.get(USGS_BASE_URL, params=params, timeout=45)
    response.raise_for_status()
    return response.json()


def write_snapshot(payload: dict[str, Any], object_key: str) -> None:
    """Persist the untouched source payload before any parsing happens."""
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    s3_client().put_object(Bucket=BUCKET, Key=object_key, Body=raw, ContentType="application/json")


def start_ingestion_run(ds: str, object_key: str, received: int, rejected: int) -> int:
    """Commit the audit row in its own transaction.

    The load transaction that follows may roll back; the attempt itself must
    remain visible. A 'running' row that never completes is the evidence of a
    crashed load, and the failure path below marks it 'failed' explicitly.
    """
    with closing(pg_connection()) as connection, connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO ops.ingestion_runs "
            "(pipeline_name, logical_date, raw_object_uri, received_count, rejected_count, status) "
            "VALUES (%s, %s, %s, %s, %s, 'running') RETURNING ingestion_run_id",
            (DAG_ID, ds, raw_object_uri(object_key), received, rejected),
        )
        run_id = int(cursor.fetchone()[0])
        connection.commit()
    return run_id


def finish_ingestion_run(run_id: int, status: str) -> None:
    with closing(pg_connection()) as connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE ops.ingestion_runs SET status = %s, completed_at = now() "
            "WHERE ingestion_run_id = %s",
            (status, run_id),
        )
        connection.commit()


def write_observations(
    run_id: int, object_key: str, valid: list[dict[str, Any]], rejected: list[tuple[str, str]]
) -> None:
    """One transaction: observation versions, quarantine rows, new watermark.

    Idempotent by construction: observations upsert on
    (event_id, source_updated_at), so replaying a logical date cannot
    duplicate versions. The watermark only advances when valid rows exist -
    an all-rejected poll must be re-fetched on the next run, not skipped over.
    """
    from psycopg2.extras import execute_values

    with closing(pg_connection()) as connection, connection.cursor() as cursor:
        if valid:
            execute_values(cursor, _INSERT_OBSERVATIONS, observation_rows(valid, object_key))
        if rejected:
            cursor.executemany(
                "INSERT INTO ops.rejected_events (ingestion_run_id, event_id, rejection_reason) "
                "VALUES (%s, %s, %s)",
                [(run_id, event_id, reason) for event_id, reason in rejected],
            )
        watermark = latest_source_updated_at(valid)
        if watermark is not None:
            cursor.execute(
                "INSERT INTO ops.ingestion_watermarks (pipeline_name, last_source_updated_at) "
                "VALUES (%s, %s) ON CONFLICT (pipeline_name) DO UPDATE "
                "SET last_source_updated_at = EXCLUDED.last_source_updated_at, updated_at = now()",
                (DAG_ID, watermark),
            )
        connection.commit()


# --- orchestration ---


def run_ingestion(ds: str) -> int:
    watermark = read_watermark()
    params = usgs_params(watermark, ds)
    logging.info("Polling USGS for ds=%s with params=%s", ds, params)
    payload = fetch_usgs_payload(params)
    features = payload.get("features", [])
    if not features:
        logging.info("No USGS observations for ds=%s; nothing to snapshot or load", ds)
        return 0

    object_key = snapshot_object_key(ds)
    write_snapshot(payload, object_key)

    valid, rejected = partition_features(features, H3_RESOLUTION)
    run_id = start_ingestion_run(ds, object_key, len(features), len(rejected))
    try:
        write_observations(run_id, object_key, valid, rejected)
    except Exception:
        finish_ingestion_run(run_id, "failed")
        raise
    finish_ingestion_run(run_id, "succeeded")
    logging.info(
        "Run %s: loaded %s valid and %s rejected observations from %s",
        run_id,
        len(valid),
        len(rejected),
        object_key,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest versioned USGS seismic observations.")
    parser.add_argument("--ds", required=True, help="Logical date to ingest (YYYY-MM-DD)")
    args = parser.parse_args(argv)
    ds = effective_logical_date(os.getenv(BACKFILL_DS_ENV), args.ds)
    return run_ingestion(ds)


if __name__ == "__main__":
    sys.exit(main())
