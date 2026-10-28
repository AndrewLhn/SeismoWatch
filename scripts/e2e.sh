#!/usr/bin/env bash
# End-to-end pipeline check against a fixed historical USGS window:
#   poll -> raw snapshot -> versioned load -> dbt marts -> freshness gate.
# The backfill date is historical on purpose: the source data cannot drift
# between runs, so every assertion is deterministic. The same script runs
# locally (make e2e) and in CI (.github/workflows/e2e.yml).
set -euo pipefail

BACKFILL_DS="${BACKFILL_DS:-2024-01-01}"
compose=(docker compose)

log() { printf '\n=== %s ===\n' "$1"; }

fail() {
  echo "E2E FAILED: $1" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs --tail=100 >&2 || true
  exit 1
}

check_eq() {
  local label="$1" actual="$2" expected="$3"
  if [[ "$actual" != "$expected" ]]; then
    fail "$label: expected '$expected', got '$actual'"
  fi
  echo "OK: $label = $actual"
}

check_gt() {
  local label="$1" actual="$2" min="$3"
  if ! [[ "$actual" =~ ^[0-9]+$ ]] || (( actual <= min )); then
    fail "$label: expected an integer > $min, got '$actual'"
  fi
  echo "OK: $label = $actual (> $min)"
}

query_scalar() {
  # psql -tA prints bare scalars; tr strips CR/LF so values compare cleanly.
  "${compose[@]}" exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1" | tr -d '\r\n'
}

run_pipeline() {
  "${compose[@]}" exec -T -e SEISMIC_BACKFILL_DS="$BACKFILL_DS" airflow-standalone \
    airflow dags test seismic_event_pipeline
}

[[ -f .env ]] || fail ".env is missing; run 'cp .env.example .env' first"
# Read the same values compose interpolates.
set -a; source .env; set +a

log "Resetting previous stack state (volumes included)"
"${compose[@]}" down -v --remove-orphans

log "Starting the stack"
# --wait blocks until every healthchecked service (including airflow-standalone)
# is ready; --wait-timeout fails fast with the stuck service named in the error.
"${compose[@]}" up -d --build --wait --wait-timeout 420

log "Waiting for Airflow to parse the DAG"
dag_ready=""
for _ in $(seq 1 60); do
  if "${compose[@]}" exec -T airflow-standalone airflow dags list 2>/dev/null | grep -q "seismic_event_pipeline"; then
    dag_ready="1"
    break
  fi
  sleep 5
done
[[ -n "$dag_ready" ]] || fail "DAG seismic_event_pipeline never showed up in 'airflow dags list'"

log "Running the pipeline once for the historical window $BACKFILL_DS"
run_pipeline

log "Asserting ingestion and audit results"
check_gt "raw_observation_versions" \
  "$(query_scalar 'SELECT count(*) FROM raw.seismic_event_observations')" 0
check_eq "succeeded_ingestion_runs" \
  "$(query_scalar "SELECT count(*) FROM ops.ingestion_runs WHERE status = 'succeeded'")" "1"
check_eq "failed_or_stuck_ingestion_runs" \
  "$(query_scalar "SELECT count(*) FROM ops.ingestion_runs WHERE status <> 'succeeded'")" "0"
check_eq "snapshot_keyed_by_logical_date" \
  "$(query_scalar "SELECT count(*) FROM ops.ingestion_runs WHERE raw_object_uri LIKE '%ingestion_date=$BACKFILL_DS%'")" "1"
check_eq "quarantine_matches_run_audit" \
  "$(query_scalar '
      SELECT count(*) FROM ops.ingestion_runs r
      LEFT JOIN (SELECT ingestion_run_id, count(*) AS c
                 FROM ops.rejected_events GROUP BY 1) q
        ON q.ingestion_run_id = r.ingestion_run_id
      WHERE r.status = '"'"'succeeded'"'"'
        AND coalesce(q.c, 0) <> r.rejected_count')" "0"
check_gt "watermark_advanced" \
  "$(query_scalar 'SELECT count(*) FROM ops.ingestion_watermarks WHERE last_source_updated_at IS NOT NULL')" 0

log "Asserting analytics models"
check_gt "current_event_versions" \
  "$(query_scalar 'SELECT count(*) FROM analytics.dim_seismic_event_current')" 0
check_gt "activity_mart_events_on_backfill_date" \
  "$(query_scalar "SELECT coalesce(sum(event_count), 0) FROM analytics.mart_daily_seismic_activity WHERE event_date = '$BACKFILL_DS'")" 0
check_eq "reliability_mart_covers_run_date" \
  "$(query_scalar "SELECT count(*) FROM analytics.mart_ingestion_reliability WHERE run_date = '$BACKFILL_DS'")" "1"

log "Proving idempotent replay: reset the watermark and re-run the same date"
first_raw="$(query_scalar 'SELECT count(*) FROM raw.seismic_event_observations')"
"${compose[@]}" exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "DELETE FROM ops.ingestion_watermarks" >/dev/null
run_pipeline
check_eq "raw_rows_unchanged_after_replay" \
  "$(query_scalar 'SELECT count(*) FROM raw.seismic_event_observations')" "$first_raw"
check_eq "replay_audited_as_second_run" \
  "$(query_scalar "SELECT count(*) FROM ops.ingestion_runs WHERE status = 'succeeded'")" "2"

log "E2E passed"
