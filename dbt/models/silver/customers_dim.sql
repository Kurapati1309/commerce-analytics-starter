with base as (
  select user_id, order_ts, total from {{ ref('orders_enriched') }}
)
select
  user_id,
  min(order_ts) as first_seen,
  max(order_ts) as last_seen,
  count(*) as lifetime_orders,
  sum(total) as lifetime_value
from base
group by 1
