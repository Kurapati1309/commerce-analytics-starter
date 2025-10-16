{{ config(materialized='table') }}

SELECT
  COALESCE(o.utm_source, 'unknown') AS channel,
  CAST(o.order_ts AS DATE)          AS order_date,
  SUM(i.qty)                        AS total_units,
  SUM(i.qty * i.unit_price)         AS total_revenue
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} i USING (order_id)
GROUP BY 1, 2
ORDER BY order_date DESC
