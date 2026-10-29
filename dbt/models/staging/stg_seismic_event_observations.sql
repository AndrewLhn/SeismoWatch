-- Typed passthrough of the append-only landing zone.
-- The event_time guard drops clock-skewed sources claiming the future;
-- contract validation at ingestion already quarantines structurally bad rows.
with source as (

    select * from {{ source('seismic_raw', 'seismic_event_observations') }}

)

select
  event_id,
  source_updated_at,
  event_time,
  magnitude,
  depth_km,
  longitude,
  latitude,
  place,
  event_status,
  magnitude_type,
  source_network,
  h3_cell,
  payload_hash,
  raw_object_uri,
  ingested_at
from source
where event_time <= now() + interval '5 minutes'
