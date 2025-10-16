with src as (
  select * from {{ source('raw', 'ad_spend_raw') }}
)
select
  cast(date as date) as date,
  lower(channel) as channel,
  lower(campaign) as campaign,
  cast(spend as decimal(18,2)) as spend,
  cast(clicks as int) as clicks,
  cast(impressions as int) as impressions
from src
