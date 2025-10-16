connection: redshift_connection
include: "views/*.view.lkml"

explore: orders_daily { }
explore: funnel_daily { }
explore: marketing_roas { }
explore: customer_ltv_30_60_90 { }
explore: inventory_health { }
