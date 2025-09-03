connection: "thelook_ecommerce"

label: "E-commerce Sandbox"

# Include all view files using standard Looker pattern
include: "/views/*.view"

# Include datagroups
include: "/datagroups.lkml"

# Datagroup definition
datagroup: thelook_ecommerce_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: thelook_ecommerce_default_datagroup

# Main explore: Order Items with comprehensive joins
explore: order_items {
  label: "Orders"
  description: "This explore allows you to analyse sales orders"
  
  join: inventory_items {
    type: left_outer
    sql_on: ${order_items.inventory_item_id} = ${inventory_items.id} ;;
    relationship: many_to_one
  }

  join: users {
    type: left_outer
    sql_on: ${order_items.user_id} = ${users.id} ;;
    relationship: many_to_one
  }

  join: products {
    type: left_outer
    sql_on: ${inventory_items.product_id} = ${products.id} ;;
    relationship: many_to_one
  }

  join: distribution_centers {
    type: left_outer
    sql_on: ${products.distribution_center_id} = ${distribution_centers.id} ;;
    relationship: many_to_one
  }
}

# Individual table explores
explore: users {
}

explore: inventory_items {
}

explore: products {
}

explore: distribution_centers {
}

explore: events {
}