{{ config(materialized='table') }}

SELECT
  CAST(o.order_ts AS DATE)          AS order_date,
  i.sku,
  SUM(i.qty)                        AS units,
  SUM(i.qty * i.unit_price)         AS revenue
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} i USING (order_id)
GROUP BY 1, 2
ORDER BY order_date, revenue DESC
