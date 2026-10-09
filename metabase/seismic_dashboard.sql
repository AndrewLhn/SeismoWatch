-- Daily activity
select * from analytics.mart_daily_seismic_activity order by event_date desc;

-- Current material events, suitable for a map question
select
  event_id, event_time, magnitude, depth_km, place,
  longitude, latitude, h3_cell
from analytics.dim_seismic_event_current
where magnitude >= 4.5
order by event_time desc;

-- Regional activity
select * from analytics.mart_h3_regional_risk
order by event_date desc, maximum_magnitude desc;
