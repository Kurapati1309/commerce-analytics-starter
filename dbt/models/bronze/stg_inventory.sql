with src as (
  select * from {{ source('raw', 'inventory_raw') }}
)
select
  cast(sku as varchar) as sku,
  warehouse,
  cast(on_hand as int) as on_hand,
  cast(reserved as int) as reserved,
  cast(updated_ts as timestamp) as updated_ts
from src
