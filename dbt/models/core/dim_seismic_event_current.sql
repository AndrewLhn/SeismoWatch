select
  event_id,
  event_time,
  source_updated_at,
  magnitude,
  depth_km,
  longitude,
  latitude,
  place,
  event_status,
  magnitude_type,
  source_network,
  h3_cell,
  event_geometry
from {{ ref('fct_seismic_event_versions') }}
where is_current_version
