{{ config(materialized='table') }}

WITH sales AS (
  SELECT
    order_date AS date,
    SUM(total_units)   AS total_units,
    SUM(total_revenue) AS total_revenue
  FROM {{ ref('fct_channel_sales') }}
  GROUP BY 1
),
returns AS (
  SELECT
    date,
    SUM(returned_units) AS returned_units,
    SUM(returned_value) AS returned_value,
    AVG(return_rate)     AS avg_return_rate
  FROM {{ ref('fct_returns_summary') }}
  GROUP BY 1
),
marketing AS (
  SELECT
    date,
    SUM(spend)   AS total_spend,
    SUM(revenue) AS marketing_revenue,
    ROUND(AVG(roas), 3) AS avg_roas
  FROM {{ ref('fct_marketing_roas') }}
  GROUP BY 1
)
SELECT
  s.date,
  s.total_units,
  s.total_revenue,
  COALESCE(r.returned_units, 0) AS returned_units,
  COALESCE(r.returned_value, 0) AS returned_value,
  COALESCE(r.avg_return_rate, 0) AS avg_return_rate,
  COALESCE(m.total_spend, 0)     AS total_spend,
  COALESCE(m.avg_roas, 0)        AS avg_roas,
  ROUND(
    CASE WHEN s.total_revenue > 0
         THEN (s.total_revenue - COALESCE(r.returned_value,0) - COALESCE(m.total_spend,0))
              / s.total_revenue
         ELSE NULL END,
  3) AS net_margin_ratio
FROM sales s
LEFT JOIN returns r USING (date)
LEFT JOIN marketing m USING (date)
ORDER BY s.date DESC
