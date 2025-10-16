with src as (
  select * from {{ source('raw', 'products_raw') }}
)
select
  cast(sku as varchar) as sku,
  title,
  lower(category) as category,
  cast(cost as decimal(18,2)) as cost,
  cast(price as decimal(18,2)) as price
from src
