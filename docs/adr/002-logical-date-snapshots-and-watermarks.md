# ADR 002: logical-date snapshots and a source-update watermark

## Decision

Raw snapshots are keyed by logical date only
(`usgs_earthquakes/ingestion_date=<ds>/events.json`); a replay of the same date overwrites the
same object. Incremental polling advances on `max(source_updated_at)` of the rows loaded, and
once a watermark exists the query uses `updatedafter` with no event-time window. A specific
date can be replayed through the `SEISMIC_BACKFILL_DS` override (`make backfill`).

## Context

USGS revises events long after they occur. Two failure modes shaped this decision:

- Constraining an incremental poll by event time (`starttime`/`endtime`) silently drops
  revisions of older events — the bug originally fixed in "ingest revisions outside the
  logical event date", now covered by a regression test.
- Keying snapshots by attempt time accumulates near-duplicate objects per logical date and
  makes replays ambiguous: nothing defines which object is the snapshot of record.

## Consequences

Replays are idempotent at both layers: the snapshot overwrites, and the
`(event_id, source_updated_at)` primary key with `ON CONFLICT DO NOTHING` makes observation
loads replayable. The trade-off is that the object store holds the latest snapshot per logical
date, not every attempt; attempt history lives in `ops.ingestion_runs`, which references the
object URI.

An empty poll exits cleanly without an audit row: absence of events is a valid outcome, while
a failed HTTP poll is an error that fails the task. The freshness SLO is enforced downstream by
`dbt source freshness` rather than by snapshot existence.

The watermark advances only when at least one valid row is loaded. An all-rejected poll keeps
the old watermark so the next run re-fetches the same window; quarantine rows preserve what
was rejected and why.
