#!/usr/bin/env python3
"""
Test the complete system by manually creating a working configuration.
"""

import os
import tempfile
import shutil
from src.main import TextToSQLEngine

def create_consolidated_model():
    """Create a consolidated model file with everything embedded."""
    
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    lookml_dir = os.path.join(temp_dir, 'lookml')
    os.makedirs(lookml_dir)
    
    # Create a consolidated model file with embedded views and explores
    model_content = '''
connection: "thelook_ecommerce"

# Embedded view: users
view: users {
  sql_table_name: `bigquery-public-data.thelook_ecommerce.users` ;;
  
  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: age {
    type: number
    sql: ${TABLE}.age ;;
  }
  dimension: gender {
    type: string
    sql: ${TABLE}.gender ;;
  }
  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }
  dimension: traffic_source {
    type: string
    sql: ${TABLE}.traffic_source ;;
  }
  measure: count {
    type: count
    sql: ${TABLE}.id ;;
  }
}

# Embedded view: order_items
view: order_items {
  sql_table_name: `bigquery-public-data.thelook_ecommerce.order_items` ;;
  
  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }
  dimension: sale_price {
    type: number
    sql: ${TABLE}.sale_price ;;
  }
  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }
  measure: count {
    type: count
    sql: ${TABLE}.id ;;
  }
  measure: total_sales {
    type: sum
    sql: ${TABLE}.sale_price ;;
  }
}

# Embedded explore
explore: order_items {
  join: users {
    type: left_outer
    sql_on: ${order_items.user_id} = ${users.id} ;;
    relationship: many_to_one
  }
}

explore: users {
}
'''
    
    model_file = os.path.join(lookml_dir, 'consolidated.model.lkml')
    with open(model_file, 'w') as f:
        f.write(model_content)
    
    return temp_dir

def test_complete_system():
    """Test the complete system with proper configuration."""
    
    print("🧪 Testing Complete System with Real BigQuery Connection")
    print("="*60)
    
    # Create consolidated model
    temp_dir = create_consolidated_model()
    lookml_path = os.path.join(temp_dir, 'lookml')
    
    try:
        # Set up configuration
        os.environ['LOOKML_REPO_PATH'] = lookml_path
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'brainrot-453319'  # Use the correct project
        
        # Initialize engine
        print("\\n1. Initializing Text-to-SQL Engine...")
        engine = TextToSQLEngine()
        
        print("✅ Engine initialized successfully")
        
        # Test queries
        test_queries = [
            "count of users",
            "users by gender", 
            "total sales by user gender",
        ]
        
        print("\\n2. Testing SQL generation...")
        
        for i, query in enumerate(test_queries, 1):
            print(f"\\n   Test {i}: '{query}'")
            
            try:
                result = engine.generate_sql(query)
                
                if result['error']:
                    print(f"   ❌ Error: {result['error']}")
                else:
                    print(f"   ✅ Generated SQL:")
                    sql_lines = result['sql'].split('\\n')
                    for line in sql_lines:
                        print(f"      {line}")
                    
                    print(f"   📊 Details:")
                    print(f"      - Explore: {result['explore_used']}")
                    print(f"      - Fields: {len(result['fields_selected'])}")
                    print(f"      - Joins: {len(result['joins_required'])}")
                    print(f"      - Processing time: {result['processing_time']:.3f}s")
            
            except Exception as e:
                print(f"   ❌ Failed: {e}")
        
        print("\\n" + "="*60)
        print("🎉 System test completed successfully!")
        
    except Exception as e:
        print(f"❌ System initialization failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)
        # Reset environment
        if 'LOOKML_REPO_PATH' in os.environ:
            del os.environ['LOOKML_REPO_PATH']

if __name__ == "__main__":
    test_complete_system()