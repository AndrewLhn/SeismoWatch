-- Every succeeded run must have quarantined exactly as many features as its
-- audit row claims. A mismatch means rejected rows were lost (or double-written)
-- relative to the run audit. Rows returned here fail the build.
with audited as (

    select ingestion_run_id, rejected_count
    from {{ ref('stg_ingestion_runs') }}
    where status = 'succeeded'

),

quarantined as (

    select ingestion_run_id, count(*) as rejected_rows
    from {{ source('seismic_ops', 'rejected_events') }}
    group by 1

)

select
    audited.ingestion_run_id,
    audited.rejected_count,
    coalesce(quarantined.rejected_rows, 0) as quarantined_rows
from audited
left join quarantined on quarantined.ingestion_run_id = audited.ingestion_run_id
where coalesce(quarantined.rejected_rows, 0) <> audited.rejected_count
