{{ config(materialized='table') }}
WITH joined AS (
  SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    i.qty,
    i.qty * i.unit_price AS line_revenue
  FROM {{ ref('stg_orders') }} o
  JOIN {{ ref('stg_order_items') }} i USING (order_id)
)
SELECT
  order_date,
  COUNT(*)          AS order_lines,
  SUM(qty)          AS units,
  SUM(line_revenue) AS gross_revenue
FROM joined
GROUP BY order_date
ORDER BY order_date
