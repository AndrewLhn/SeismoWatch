# Architecture

SeismoWatch ingests USGS seismic observations and retains both immutable source snapshots and
versioned relational records.

~~~text
USGS FDSN API -> Airflow -> S3-compatible raw snapshot -> PostgreSQL/PostGIS -> dbt -> Metabase
~~~

## Runtime boundary

A single-host Docker Compose stack for local development:

- **Airflow 3 standalone** runs the scheduler, API server, and DAG processor in one container.
  Ingestion logic lives in an Airflow-free module (`dags/seismic_pipeline_run.py`), invoked
  through a BashOperator, so the whole pipeline is unit-testable without an orchestrator.
- **dbt runs in an isolated venv** inside the Airflow image (`dbt_env`); its pins cannot
  break the scheduler's Python environment.
- **Silo** (a pinned MinIO-compatible fork) provides the S3 API for raw snapshots; upstream
  MinIO images were removed from registries in 2026.
- Published ports bind to loopback only; secrets come from a required, git-ignored `.env`.

## Event lifecycle

1. The run resolves its logical date (scheduler `ds`, or an explicit backfill override).
2. It reads the last loaded `source_updated_at` watermark and builds the FDSN query:
   an event-time window on the first run, `updatedafter` afterwards (ADR-002).
3. The complete GeoJSON response is written to the object store before transformation,
   under a key scoped to the logical date, so replays overwrite rather than accumulate.
4. The run-audit row is committed first; each feature is then validated, normalised,
   H3-indexed, and written as an observation version in one transaction with the
   quarantine rows and the new watermark. Failures mark the audit row `failed`.
5. dbt builds history, current-state, activity, regional-risk, and reliability models,
   and the run ends on a `dbt source freshness` gate.

The source can revise an existing event. The primary key is `(event_id, source_updated_at)`,
so revisions are retained while `dim_seismic_event_current` exposes the latest version.

## Quality and recovery

- Malformed features are quarantined in `ops.rejected_events`; valid events in the same poll
  continue, and a singular dbt test asserts quarantine counts match the run audit.
- Each attempt records received/rejected counts and terminal status in `ops.ingestion_runs`,
  aggregated daily by `analytics.mart_ingestion_reliability` together with watermark age.
- Re-running a logical date is safe end to end: the snapshot key is stable, observation
  inserts are idempotent, and the watermark only advances on valid rows.
- Stale landing zones fail the pipeline through source freshness; the
  [runbook](runbooks/usgs-source-freshness.md) covers triage.
