datagroup: orders_datagroup {
  sql_trigger: SELECT MAX(created_at) FROM `bigquery-public-data.thelook_ecommerce.events`;;
  max_cache_age: "24 hours"
  label: "ETL ID added"
  description: "Triggered when new ID is added to ETL log"
}