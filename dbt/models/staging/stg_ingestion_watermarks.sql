-- Source-update high-water marks per pipeline.
with source as (

    select * from {{ source('seismic_ops', 'ingestion_watermarks') }}

)

select
  pipeline_name,
  last_source_updated_at,
  updated_at
from source
