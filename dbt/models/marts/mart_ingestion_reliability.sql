-- Daily ingestion reliability: run outcomes, rejection rate, and how far the
-- source watermark trails the present. This is the durable SLO record - it
-- answers "did we load, did anything fail validation, how stale is the source"
-- without depending on scheduler history retention.
with daily_runs as (

    select
        logical_date as run_date,
        count(*) as run_count,
        count(*) filter (where status = 'succeeded') as succeeded_runs,
        count(*) filter (where status = 'failed') as failed_runs,
        coalesce(sum(received_count), 0) as received_observations,
        coalesce(sum(rejected_count), 0) as rejected_observations
    from {{ ref('stg_ingestion_runs') }}
    group by 1

),

watermark as (

    select
        max(last_source_updated_at) as watermark_at
    from {{ ref('stg_ingestion_watermarks') }}

)

select
    daily_runs.run_date,
    daily_runs.run_count,
    daily_runs.succeeded_runs,
    daily_runs.failed_runs,
    daily_runs.received_observations,
    daily_runs.rejected_observations,
    round(
        100.0 * daily_runs.rejected_observations / nullif(daily_runs.received_observations, 0),
        2
    ) as rejection_rate_pct,
    watermark.watermark_at,
    round(extract(epoch from (now() - watermark.watermark_at))::numeric / 3600, 1)
        as watermark_age_hours
from daily_runs
cross join watermark
