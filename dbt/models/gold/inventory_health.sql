with snap as (
  select * from {{ ref('inventory_snapshot') }}
),
agg as (
  select
    sku,
    warehouse,
    on_hand - reserved as available
  from snap
)
select * from agg
