with rev as (
  select date, channel, sum(gmv) as revenue
  from {{ ref('orders_daily') }}
  group by 1,2
),
spend as (
  select date, channel, sum(spend) as spend
  from {{ ref('marketing_spend_daily') }}
  group by 1,2
)
select
  coalesce(rev.date, spend.date) as date,
  coalesce(rev.channel, spend.channel) as channel,
  coalesce(revenue,0) as revenue,
  coalesce(spend,0) as spend,
  case when spend=0 then null else round(revenue/spend,2) end as roas
from rev full outer join spend using (date, channel)
