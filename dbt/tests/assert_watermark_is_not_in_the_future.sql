-- The watermark tracks source publication time; a future value means clock
-- skew or a mis-parse in the contract layer. Rows returned here fail the build.
select *
from {{ ref('stg_ingestion_watermarks') }}
where last_source_updated_at > now() + interval '5 minutes'
