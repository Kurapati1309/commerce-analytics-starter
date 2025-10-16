with s as (
  select * from {{ ref('events_sessionized') }}
),
daily as (
  select
    cast(date_trunc('day', session_start) as date) as date,
    coalesce(utm_source,'(none)') as channel,
    coalesce(device,'(unknown)') as device,
    sum(page_views) as sessions,
    sum(adds) as adds,
    sum(checkouts) as checkouts,
    sum(purchases) as purchases
  from s
  group by 1,2,3
)
select
  *,
  case when sessions=0 then 0 else round(purchases::decimal/sessions,4) end as cvr
from daily
