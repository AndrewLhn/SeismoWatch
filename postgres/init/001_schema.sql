CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS ops.ingestion_watermarks (
  pipeline_name text PRIMARY KEY,
  last_source_updated_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.ingestion_runs (
  ingestion_run_id bigserial PRIMARY KEY,
  pipeline_name text NOT NULL,
  logical_date date NOT NULL,
  raw_object_uri text NOT NULL,
  received_count integer NOT NULL,
  rejected_count integer NOT NULL,
  status text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS ops.rejected_events (
  rejected_event_id bigserial PRIMARY KEY,
  ingestion_run_id bigint NOT NULL REFERENCES ops.ingestion_runs(ingestion_run_id),
  event_id text NOT NULL,
  rejection_reason text NOT NULL,
  rejected_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.seismic_event_observations (
  event_id text NOT NULL,
  source_updated_at timestamptz NOT NULL,
  event_time timestamptz NOT NULL,
  magnitude double precision,
  depth_km double precision,
  longitude double precision NOT NULL,
  latitude double precision NOT NULL,
  place text,
  event_status text,
  magnitude_type text,
  source_network text,
  h3_cell text NOT NULL,
  payload_hash text NOT NULL,
  raw_payload jsonb NOT NULL,
  raw_object_uri text NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, source_updated_at)
);

CREATE INDEX IF NOT EXISTS seismic_observations_event_time_idx ON raw.seismic_event_observations (event_time);
CREATE INDEX IF NOT EXISTS seismic_observations_h3_idx ON raw.seismic_event_observations (h3_cell);
