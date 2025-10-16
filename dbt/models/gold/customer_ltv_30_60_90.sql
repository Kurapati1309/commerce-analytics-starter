with base as (
  select * from {{ ref('orders_enriched') }}
),
first_order as (
  select user_id, min(order_ts) as cohort_start
  from base group by 1
),
joined as (
  select b.user_id, b.order_ts, b.total, f.cohort_start
  from base b join first_order f using (user_id)
),
windows as (
  select
    date_trunc('month', cohort_start) as cohort_month,
    sum(case when order_ts <= cohort_start + interval '30' day then total else 0 end) as ltv_30,
    sum(case when order_ts <= cohort_start + interval '60' day then total else 0 end) as ltv_60,
    sum(case when order_ts <= cohort_start + interval '90' day then total else 0 end) as ltv_90
  from joined
  group by 1
)
select * from windows
