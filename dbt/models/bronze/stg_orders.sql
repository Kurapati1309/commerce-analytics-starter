with src as (
  select * from {{ source('raw', 'orders_raw') }}
)
select
  cast(order_id as varchar) as order_id,
  cast(user_id as varchar) as user_id,
  cast(order_ts as timestamp) as order_ts,
  cast(subtotal as decimal(18,2)) as subtotal,
  cast(discount as decimal(18,2)) as discount,
  cast(tax as decimal(18,2)) as tax,
  cast(shipping as decimal(18,2)) as shipping,
  cast(total as decimal(18,2)) as total,
  lower(status) as status,
  nullif(coupon_code,'') as coupon_code,
  lower(nullif(utm_source,'')) as utm_source,
  lower(nullif(utm_campaign,'')) as utm_campaign
from src
