with src as (
  select * from {{ source('raw', 'returns_raw') }}
)
select
  cast(return_id as varchar) as return_id,
  cast(order_id as varchar) as order_id,
  cast(sku as varchar) as sku,
  cast(qty as int) as qty,
  reason,
  cast(processed_ts as timestamp) as processed_ts,
  cast(amount_refunded as decimal(18,2)) as amount_refunded
from src
