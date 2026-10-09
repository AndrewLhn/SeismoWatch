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
from raw.seismic_event_observations
where event_time <= now() + interval '5 minutes'
