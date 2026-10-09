select
  *,
  row_number() over (
    partition by event_id
    order by source_updated_at desc, ingested_at desc
  ) = 1 as is_current_version,
  st_setsrid(st_makepoint(longitude, latitude), 4326)::geometry(Point, 4326) as event_geometry
from {{ ref('stg_seismic_event_observations') }}
