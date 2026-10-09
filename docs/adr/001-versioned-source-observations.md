# ADR 001: retain source revisions as observations

## Decision

Store USGS records append-only by `event_id` and `source_updated_at`. Build a separate
current-state projection in dbt.

## Context

USGS may revise magnitude, depth, location, and status after an event first appears. A simple
upsert would discard this history and make investigation impossible.

## Consequences

The model supports audit, replay, and revision analysis. Consumers who need only the present
state query `dim_seismic_event_current`; historical analysis uses
`fct_seismic_event_versions`.
