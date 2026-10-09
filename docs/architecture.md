# Architecture

SeismoWatch ingests USGS seismic observations and retains both immutable source snapshots and
versioned relational records.

~~~text
USGS FDSN API -> Airflow -> MinIO raw snapshot -> PostgreSQL/PostGIS raw observations -> dbt models
~~~

## Event lifecycle

1. The pipeline reads the last successful `source_updated_at` watermark.
2. It requests USGS observations for the logical date and later source updates.
3. The complete GeoJSON response is written to MinIO before transformation.
4. Each feature is validated, normalised, H3-indexed, and written as an observation version.
5. dbt builds history, current-state, daily-activity, and H3 regional-risk models.

The source can revise an existing event. The primary key is `(event_id, source_updated_at)`, so
revisions are retained while `dim_seismic_event_current` exposes the latest version.

## Quality and recovery

Malformed features are stored in `ops.rejected_events`; valid events continue. Each run records
input and rejection counts in `ops.ingestion_runs`. Re-run a logical date to replay its raw
snapshot flow; raw objects retain the original payload for audit.
