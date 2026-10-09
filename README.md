# SeismoWatch

Local reference platform for ingesting, versioning, and analysing USGS seismic-event data.

~~~text
USGS FDSN API -> Airflow -> MinIO raw snapshots -> PostgreSQL / PostGIS -> dbt -> Metabase
~~~

USGS can revise an event after its first publication. SeismoWatch keeps immutable source snapshots
and append-only observation versions, then exposes both historical and current-state models.

## What it demonstrates

- Incremental API polling with a `source_updated_at` watermark.
- Immutable GeoJSON raw snapshots in MinIO.
- Contract validation, rejected-event audit records, and ingestion-run metadata.
- Idempotent relational ingestion keyed by `event_id` and source update time.
- PostGIS geometry and H3 spatial indexing.
- dbt models for version history, current state, daily activity, and regional risk.
- Unit tests, linting, and CI for source normalisation.
- A compact Metabase query pack for operations and risk views.

## Run locally

Requirements: Docker and Docker Compose.

~~~bash
cp .env.example .env
# Replace placeholder passwords.
docker compose up -d --build
~~~

Services:

| Service | Address |
| --- | --- |
| Airflow | http://localhost:8080 |
| MinIO | http://localhost:9001 |
| Metabase | http://localhost:3000 |
| PostgreSQL / PostGIS | localhost:5432 |

Trigger `seismic_event_pipeline` in Airflow. A successful run creates an immutable raw object,
loads valid source versions, records rejected rows, advances the watermark, and executes
`dbt build`.

## Data model

| Model | Purpose |
| --- | --- |
| `raw.seismic_event_observations` | Versioned source observations |
| `analytics.fct_seismic_event_versions` | Event revision history plus PostGIS geometry |
| `analytics.dim_seismic_event_current` | Latest known state for each event |
| `analytics.mart_daily_seismic_activity` | Daily counts and magnitude statistics |
| `analytics.mart_h3_regional_risk` | H3-cell activity and material-event metrics |
| `ops.ingestion_runs` | Input, rejected-row, and completion audit trail |

## Operational rules

- An empty seismic day is a valid outcome and skips downstream transformation.
- A failed API poll or non-advancing watermark is a freshness incident.
- Invalid features are quarantined in `ops.rejected_events`; they do not discard valid events.
- Revisions are retained; consumers use the current-state model only when they do not need history.

## Checks

~~~bash
python -m unittest discover -s tests -v
ruff check dags/seismic_contract.py tests
~~~

The dashboard query pack is in [metabase/seismic_dashboard.sql](metabase/seismic_dashboard.sql).

## Documentation

- [Architecture](docs/architecture.md)
- [Versioned-observations ADR](docs/adr/001-versioned-source-observations.md)
- [USGS freshness runbook](docs/runbooks/usgs-source-freshness.md)
