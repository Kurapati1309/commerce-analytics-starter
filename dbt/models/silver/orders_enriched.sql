with o as (select * from {{ ref('stg_orders') }}),
oi as (select * from {{ ref('stg_order_items') }})
select
  o.order_id,
  o.user_id,
  o.order_ts,
  sum(oi.line_total) as items_total,
  o.subtotal, o.discount, o.tax, o.shipping, o.total,
  o.status, o.coupon_code, o.utm_source, o.utm_campaign
from o
left join oi on oi.order_id = o.order_id
group by 1,2,3,5,6,7,8,9,10,11,12
