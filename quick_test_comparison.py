#!/usr/bin/env python3
"""Quick comparison test between original and enhanced system."""

import sys
import os
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Minimal logging
logging.basicConfig(level=logging.ERROR)

def test_problematic_query():
    """Test the specific query that was problematic in the original analysis."""
    
    print("🔍 Quick Test: Revenue by Product Category")
    print("=" * 50)
    
    from src.main import TextToSQLEngine
    
    # Test query that was failing semantically
    test_query = "Show me revenue by product category"
    
    print(f"Query: {test_query}")
    print("Expected: Should use actual transaction values, not catalog prices")
    print("-" * 50)
    
    try:
        # Initialize with enhanced context (default)
        engine = TextToSQLEngine()
        
        print("🚀 Testing with Enhanced Semantic System...")
        result = engine.generate_sql(test_query)
        
        if result['error']:
            print(f"❌ Error: {result['error']}")
        else:
            sql = result['sql'].lower()
            print("Generated SQL:")
            print(result['sql'])
            
            # Analyze the result
            print("\n📊 Analysis:")
            
            if 'sale_price' in sql:
                print("✅ SUCCESS: Uses actual transaction values (sale_price)")
                print("   This represents real revenue from completed sales")
            elif 'retail_price' in sql or 'product_retail_price' in sql:
                print("❌ SEMANTIC ERROR: Uses catalog/retail prices")
                print("   This would show inventory value, not actual revenue")
            else:
                print("❓ Unknown revenue field used")
            
            print(f"\n⚡ Performance:")
            print(f"   Processing time: {result['processing_time']:.2f}s")
            print(f"   LLM cost: ${result.get('llm_cost_estimate', 0):.6f}")
            print(f"   Tables considered: {result.get('explore_used', 'Unknown')}")
            
    except Exception as e:
        print(f"💥 Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_problematic_query()