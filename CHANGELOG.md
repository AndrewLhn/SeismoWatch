# Changelog

Notable changes to this project. Format loosely follows Keep a Changelog.

## Unreleased

### Changed

- Airflow 2.10.5 -> 3.3.2: the 2.x line is EOL. The stack now runs a single
  `airflow-standalone` service (init/webserver/scheduler collapsed, the shape
  Ledgerline uses) and the admin password is generated on first start:
  `docker compose exec airflow-standalone cat /opt/airflow/standalone_admin_password.txt`.
  `AIRFLOW_ADMIN_PASSWORD` is gone from `.env.example`.
- Object store: upstream MinIO was archived and its images removed from Docker
  Hub and Quay in 2026, so `docker compose up` could no longer pull them. The
  stack now runs the pinned `pgsty/silo` MinIO-compatible fork, and bucket
  bootstrap uses a plain S3 call (`infra/minio/init-bucket.py`) instead of the
  discontinued `mc` client. Recreate local volumes once: `docker compose down -v`.
- dbt runs in an isolated venv inside the Airflow image (`dbt_env`, pins in
  `dbt/requirements.txt`), so a dbt bump cannot break the scheduler;
  dbt-core 1.8.9 -> 1.11.16, dbt-postgres -> 1.11.0.
- Ingestion logic moved out of the DAG into `dags/seismic_pipeline_run.py`,
  which imports no Airflow modules; the DAG is a thin BashOperator shell.
  Task IDs changed: `snapshot_usgs_events` + `load_versioned_observations`
  are now a single `ingest_usgs_observations`.
- An empty poll exits clean instead of raising an Airflow skip, and dbt models
  rebuild regardless so marts stay consistent (ADR-002).
- Staging models read through `{{ source() }}`; freshness thresholds live in
  `dbt/models/staging/sources.yml`.
- Compose secrets use `:?` interpolation (fail fast without `.env`), published
  ports bind to loopback only, and Airflow logs live in a named volume.
- Runtime dependencies bumped to current pins (boto3, h3, psycopg2-binary,
  requests); dependabot now tracks pip, dbt, docker, and GitHub Actions.

### Added

- `check_source_freshness` task: `dbt source freshness` gates every run on the
  raw and ops sources (warn 36h, error 72h) - freshness is now enforced, not
  just described in the runbook.
- `mart_ingestion_reliability`: daily run outcomes, rejection rate, and
  watermark age; exposed in the Metabase query pack.
- Singular dbt tests: the watermark is never in the future, and quarantine row
  counts match the run audit for succeeded runs.
- End-to-end check (`make e2e`, `.github/workflows/e2e.yml`): boots the stack,
  replays a fixed historical USGS window through the real DAG, and asserts on
  raw versions, run audit, quarantine consistency, marts, and idempotent replay.
- Backfill path: `make backfill BACKFILL_DS=YYYY-MM-DD` (env override
  `SEISMIC_BACKFILL_DS`, ADR-002).
- CI: coverage reporting, `dbt parse`, compose config validation, lint over
  all sources, `permissions`/`concurrency`/timeout guards.
- Makefile, pre-commit config, pyproject (ruff/coverage), this changelog,
  and ADR-002.
- Unit tests for query-window construction (including the
  revisions-outside-the-logical-date regression), snapshot keys, feature
  quarantine, row mapping, run-audit outcomes, and DAG structure.

### Fixed

- A failed load no longer erases its own audit trail: the `ops.ingestion_runs`
  row is committed before the load transaction and marked `failed` on error.
  Previously the entire attempt rolled back and the run was invisible.
- Runs where every feature is rejected now record the quarantine rows and the
  run audit instead of skipping before writing anything; the watermark only
  advances on valid rows.
- `ops.ingestion_runs.status` is constrained to running/succeeded/failed, and
  `ops.rejected_events` got an index on its run FK (schema applies to fresh
  volumes).
