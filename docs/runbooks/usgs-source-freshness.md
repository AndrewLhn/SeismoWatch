# Runbook: USGS source freshness incident

## Detection

Automated, in this order of escalation:

1. `check_source_freshness` fails the DAG run when a source crosses its error threshold
   (72h without new loaded rows; warn at 36h) — configured in
   `dbt/models/staging/sources.yml`.
2. `analytics.mart_ingestion_reliability` shows the durable picture: `failed_runs`,
   `watermark_age_hours`, and rejection rate per run date.
3. A stuck `running` row in `ops.ingestion_runs` means a load crashed mid-transaction.

## Triage

1. Check the Airflow task logs for the failed run and the USGS endpoint response.
2. Check whether the raw object exists in the bucket for the logical date
   (`usgs_earthquakes/ingestion_date=<ds>/events.json`).
3. Check `ops.rejected_events` and `rejected_count` for a contract or source-format change:
   a spike in rejections with a healthy poll usually means USGS changed the payload, and the
   contract layer is doing its job.
4. Do not treat an empty event day as an incident: an empty poll exits clean and writes no
   audit row. Only a failed poll, a stuck `running` row, or a non-advancing watermark counts.

## Recovery

Fix configuration or contract handling, then replay the logical date:

~~~bash
make backfill BACKFILL_DS=<YYYY-MM-DD>
~~~

The replay is idempotent (ADR-002): the snapshot overwrites, observation inserts deduplicate,
and the watermark only advances on valid rows. Verify that the run is marked `succeeded` in
`ops.ingestion_runs`, the watermark advanced, dbt models built, and
`check_source_freshness` passed.
