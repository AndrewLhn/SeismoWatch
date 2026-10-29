-- Per-attempt ingestion audit, including failed runs: a 'running' row that
-- never completed is itself evidence of a crashed load.
with source as (

    select * from {{ source('seismic_ops', 'ingestion_runs') }}

)

select
  ingestion_run_id,
  pipeline_name,
  logical_date,
  raw_object_uri,
  received_count,
  rejected_count,
  status,
  started_at,
  completed_at
from source
