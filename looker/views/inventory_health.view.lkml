view: inventory_health {
  sql_table_name: analytics.inventory_health ;;
  dimension: sku { sql: ${TABLE}.sku ;;}
  dimension: warehouse { sql: ${TABLE}.warehouse ;;}
  measure: available { type: sum sql: ${TABLE}.available ;;}
}
