view: orders_daily {
  sql_table_name: analytics.orders_daily ;;
  dimension_group: date {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.date ;;
  }
  dimension: channel { sql: ${TABLE}.channel ;;}
  measure: orders { type: sum sql: ${TABLE}.orders ;;}
  measure: gmv { type: sum sql: ${TABLE}.gmv ;;}
  measure: aov { type: average sql: ${TABLE}.aov ;;}
}
