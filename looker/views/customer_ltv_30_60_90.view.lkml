view: customer_ltv_30_60_90 {
  sql_table_name: analytics.customer_ltv_30_60_90 ;;
  dimension_group: cohort_month { type: time timeframes: [month] sql: ${TABLE}.cohort_month ;;}
  measure: ltv_30 { type: sum sql: ${TABLE}.ltv_30 ;;}
  measure: ltv_60 { type: sum sql: ${TABLE}.ltv_60 ;;}
  measure: ltv_90 { type: sum sql: ${TABLE}.ltv_90 ;;}
}
