with o as (
  select * from {{ ref('orders_enriched') }}
)
select
  cast(date_trunc('day', order_ts) as date) as date,
  coalesce(utm_source,'(none)') as channel,
  count(*) as orders,
  sum(total) as gmv,
  case when count(*)=0 then 0 else round(sum(total)/count(*),2) end as aov
from o
group by 1,2
