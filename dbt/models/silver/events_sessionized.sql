with base as (
  select * from {{ ref('stg_events') }}
),
marked as (
  select
    *,
    case when lag(event_ts) over (partition by user_id order by event_ts) is null
      or datediff(minute, lag(event_ts) over (partition by user_id order by event_ts), event_ts) > 30
      then 1 else 0 end as new_session_flag
  from base
),
sessions as (
  select
    *,
    sum(new_session_flag) over (partition by user_id order by event_ts rows unbounded preceding) as session_num
  from marked
)
select
  user_id,
  concat(user_id,'-',session_num) as session_key,
  min(event_ts) as session_start,
  max(event_ts) as session_end,
  count_if(event_type='page_view') as page_views,
  count_if(event_type='add_to_cart') as adds,
  count_if(event_type='checkout') as checkouts,
  count_if(event_type='purchase') as purchases,
  any_value(device) as device,
  any_value(utm_source) as utm_source,
  any_value(utm_campaign) as utm_campaign
from sessions
group by 1,2
