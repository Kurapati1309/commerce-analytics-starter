select date, channel, campaign, spend, clicks, impressions
from {{ ref('stg_ad_spend') }}
