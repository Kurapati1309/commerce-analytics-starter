view: funnel_daily {
  sql_table_name: analytics.funnel_daily ;;
  dimension_group: date { type: time timeframes: [date, week, month] sql: ${TABLE}.date ;;}
  dimension: channel { sql: ${TABLE}.channel ;;}
  dimension: device { sql: ${TABLE}.device ;;}
  measure: sessions { type: sum sql: ${TABLE}.sessions ;;}
  measure: adds { type: sum sql: ${TABLE}.adds ;;}
  measure: checkouts { type: sum sql: ${TABLE}.checkouts ;;}
  measure: purchases { type: sum sql: ${TABLE}.purchases ;;}
  measure: cvr { type: average sql: ${TABLE}.cvr ;;}
}
