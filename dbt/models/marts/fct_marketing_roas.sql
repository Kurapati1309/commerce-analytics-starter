{{ config(materialized='table') }}

WITH channel_daily AS (
  SELECT
    channel,
    order_date,
    SUM(total_revenue) AS revenue
  FROM {{ ref('fct_channel_sales') }}
  GROUP BY 1, 2
),
spend_daily AS (
  SELECT
    COALESCE(channel, 'unknown') AS channel,
    CAST(date AS DATE)           AS spend_date,
    SUM(spend)                   AS spend
  FROM {{ source('raw','ad_spend_raw') }}
  GROUP BY 1, 2
)

SELECT
  s.spend_date                    AS date,
  s.channel                       AS channel,   -- qualify!
  s.spend,
  COALESCE(c.revenue, 0)          AS revenue,
  ROUND(CASE WHEN s.spend > 0 THEN COALESCE(c.revenue,0) / s.spend ELSE NULL END, 3) AS roas
FROM spend_daily s
LEFT JOIN channel_daily c
  ON c.channel = s.channel
 AND c.order_date = s.spend_date
ORDER BY date DESC, channel
