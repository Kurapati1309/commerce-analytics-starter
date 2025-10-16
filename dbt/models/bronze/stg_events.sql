with src as (
  select * from {{ source('raw', 'events_stream') }}
)
select
  cast(event_id as varchar) as event_id,
  cast(user_id as varchar) as user_id,
  cast(session_id as varchar) as session_id,
  cast(event_ts as timestamp) as event_ts,
  lower(event_type) as event_type,
  lower(device) as device,
  page_url,
  lower(nullif(utm_source,'')) as utm_source,
  lower(nullif(utm_campaign,'')) as utm_campaign
from src
