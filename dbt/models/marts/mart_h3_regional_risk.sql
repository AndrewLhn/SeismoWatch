select
  h3_cell,
  event_time::date as event_date,
  count(*) as event_count,
  count(*) filter (where magnitude >= 4.5) as material_event_count,
  max(magnitude) as maximum_magnitude,
  avg(depth_km) as average_depth_km
from {{ ref('dim_seismic_event_current') }}
group by 1, 2
