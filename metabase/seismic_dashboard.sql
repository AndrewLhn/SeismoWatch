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

-- Ingestion reliability, last 14 run dates
select
  run_date, run_count, succeeded_runs, failed_runs,
  received_observations, rejected_observations, rejection_rate_pct,
  watermark_at, watermark_age_hours
from analytics.mart_ingestion_reliability
order by run_date desc
limit 14;

-- Quarantine detail for the most recent run
select r.started_at, r.logical_date, r.status, e.event_id, e.rejection_reason
from ops.rejected_events e
join ops.ingestion_runs r on r.ingestion_run_id = e.ingestion_run_id
order by r.started_at desc, e.rejected_event_id desc
limit 100;
