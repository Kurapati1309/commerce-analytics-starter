view: marketing_roas {
  sql_table_name: analytics.marketing_roas ;;
  dimension_group: date { type: time timeframes: [date, week, month] sql: ${TABLE}.date ;;}
  dimension: channel { sql: ${TABLE}.channel ;;}
  measure: revenue { type: sum sql: ${TABLE}.revenue ;;}
  measure: spend { type: sum sql: ${TABLE}.spend ;;}
  measure: roas { type: average sql: ${TABLE}.roas ;;}
}
