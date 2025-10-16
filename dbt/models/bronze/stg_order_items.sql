with src as (
  select * from {{ source('raw', 'order_items_raw') }}
)
select
  cast(order_item_id as varchar) as order_item_id,
  cast(order_id as varchar) as order_id,
  cast(sku as varchar) as sku,
  cast(qty as int) as qty,
  cast(unit_price as decimal(18,2)) as unit_price,
  cast(line_total as decimal(18,2)) as line_total
from src
