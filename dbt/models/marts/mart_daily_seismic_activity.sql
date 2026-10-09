select
  event_time::date as event_date,
  count(*) as event_count,
  count(*) filter (where magnitude >= 5) as high_magnitude_event_count,
  round(avg(magnitude)::numeric, 2) as average_magnitude,
  round(max(magnitude)::numeric, 2) as maximum_magnitude,
  round(avg(depth_km)::numeric, 2) as average_depth_km
from {{ ref('dim_seismic_event_current') }}
group by 1
