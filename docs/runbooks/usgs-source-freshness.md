# Runbook: USGS source freshness incident

## Trigger

The expected daily run has no successful record in `ops.ingestion_runs`, or the watermark has
not advanced within the agreed observation window.

## Triage

1. Check the Airflow task logs and the USGS endpoint response.
2. Check whether the raw object exists in MinIO for the logical date.
3. Check `ops.rejected_events` for a contract or source-format change.
4. Do not treat an empty event day as an incident unless the API poll itself failed.

## Recovery

Fix configuration or contract handling, then clear and re-run the failed logical date. Verify
that the run is marked `succeeded`, the watermark advances, and dbt models build successfully.
