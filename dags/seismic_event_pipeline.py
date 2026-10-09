"""Daily ingestion of versioned USGS seismic-event observations."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import boto3
import psycopg2
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from psycopg2.extras import Json, execute_values

from seismic_contract import EventValidationError, normalise_feature

DAG_ID = "seismic_event_pipeline"
BUCKET = os.getenv("SEISMIC_BUCKET", "seismic-raw")
USGS_BASE_URL = os.getenv("USGS_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
H3_RESOLUTION = int(os.getenv("SEISMIC_H3_RESOLUTION", "5"))


def pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def read_watermark() -> datetime | None:
    with pg_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT last_source_updated_at FROM ops.ingestion_watermarks WHERE pipeline_name = %s",
            (DAG_ID,),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def snapshot_from_usgs(ds: str, **_: object) -> dict[str, str]:
    watermark = read_watermark()
    params = {
        "format": "geojson",
        "starttime": ds,
        "endtime": (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"),
        "orderby": "time-asc",
    }
    if watermark:
        params["updatedafter"] = watermark.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    response = requests.get(USGS_BASE_URL, params=params, timeout=45)
    response.raise_for_status()
    payload = response.json()
    features = payload.get("features", [])
    if not features:
        raise AirflowSkipException(f"No USGS observations for {ds}")

    key = f"usgs_earthquakes/ingestion_date={ds}/events.json"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType="application/json")
    return {"object_key": key, "feature_count": str(len(features))}


def load_observations(ti, ds: str, **_: object) -> None:
    snapshot = ti.xcom_pull(task_ids="snapshot_usgs_events")
    object_key = snapshot["object_key"]
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )
    payload = json.loads(s3.get_object(Bucket=BUCKET, Key=object_key)["Body"].read())
    valid: list[dict[str, Any]] = []
    rejected: list[tuple[str, str]] = []

    for feature in payload.get("features", []):
        try:
            valid.append(normalise_feature(feature, H3_RESOLUTION))
        except EventValidationError as error:
            rejected.append((str(feature.get("id", "unknown")), str(error)))

    if not valid:
        raise AirflowSkipException(f"No valid observations in {object_key}")

    values = [
        (
            event["event_id"], event["source_updated_at"], event["event_time"], event["magnitude"],
            event["depth_km"], event["longitude"], event["latitude"], event["place"], event["event_status"],
            event["magnitude_type"], event["source_network"], event["h3_cell"], event["payload_hash"],
            event["raw_payload"], f"s3://{BUCKET}/{object_key}",
        )
        for event in valid
    ]
    with pg_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO ops.ingestion_runs (pipeline_name, logical_date, raw_object_uri, received_count, rejected_count, status) "
            "VALUES (%s, %s, %s, %s, %s, 'running') RETURNING ingestion_run_id",
            (DAG_ID, ds, f"s3://{BUCKET}/{object_key}", len(payload.get("features", [])), len(rejected)),
        )
        run_id = cursor.fetchone()[0]
        execute_values(
            cursor,
            "INSERT INTO raw.seismic_event_observations "
            "(event_id, source_updated_at, event_time, magnitude, depth_km, longitude, latitude, place, event_status, "
            "magnitude_type, source_network, h3_cell, payload_hash, raw_payload, raw_object_uri) VALUES %s "
            "ON CONFLICT (event_id, source_updated_at) DO NOTHING",
            values,
        )
        cursor.executemany(
            "INSERT INTO ops.rejected_events (ingestion_run_id, event_id, rejection_reason) VALUES (%s, %s, %s)",
            [(run_id, event_id, reason) for event_id, reason in rejected],
        )
        watermark = max(event["source_updated_at"] for event in valid)
        cursor.execute(
            "INSERT INTO ops.ingestion_watermarks (pipeline_name, last_source_updated_at) VALUES (%s, %s) "
            "ON CONFLICT (pipeline_name) DO UPDATE SET last_source_updated_at = EXCLUDED.last_source_updated_at, updated_at = now()",
            (DAG_ID, watermark),
        )
        cursor.execute(
            "UPDATE ops.ingestion_runs SET status = 'succeeded', completed_at = now() WHERE ingestion_run_id = %s",
            (run_id,),
        )
    logging.info("Loaded %s valid and %s rejected observations from %s", len(valid), len(rejected), object_key)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 3, "retry_delay": timedelta(minutes=10)},
    max_active_runs=1,
    tags=["seismic", "usgs", "geospatial"],
    description="Snapshots, versions, and models USGS seismic-event observations.",
) as dag:
    snapshot = PythonOperator(task_id="snapshot_usgs_events", python_callable=snapshot_from_usgs)
    load = PythonOperator(task_id="load_versioned_observations", python_callable=load_observations)
    transform = BashOperator(
        task_id="build_analytics_models",
        bash_command="cd /opt/airflow/dbt && dbt build --profiles-dir .",
    )
    snapshot >> load >> transform
