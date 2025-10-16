{{ config(materialized='table') }}

WITH returns AS (
  SELECT
    CAST(processed_ts AS DATE)      AS return_date,
    order_id,
    sku,
    SUM(qty)                        AS returned_units,
    SUM(amount_refunded)            AS returned_value
  FROM {{ source('raw','returns_raw') }}
  GROUP BY 1,2,3
),
orders AS (
  SELECT
    CAST(order_ts AS DATE)          AS order_date,
    order_id
  FROM {{ ref('stg_orders') }}
),
daily_orders AS (
  SELECT order_date, COUNT(*) AS orders
  FROM orders
  GROUP BY 1
),
daily_returns AS (
  SELECT return_date, 
         SUM(returned_units) AS returned_units, 
         SUM(returned_value) AS returned_value
  FROM returns
  GROUP BY 1
)
SELECT
  d.order_date AS date,
  d.orders,
  COALESCE(r.returned_units, 0) AS returned_units,
  COALESCE(r.returned_value, 0) AS returned_value,
  ROUND(
    CASE 
      WHEN d.orders > 0 THEN COALESCE(r.returned_units,0) / d.orders::DOUBLE 
      ELSE NULL 
    END, 
  3) AS return_rate
FROM daily_orders d
LEFT JOIN daily_returns r ON r.return_date = d.order_date
ORDER BY date DESC
