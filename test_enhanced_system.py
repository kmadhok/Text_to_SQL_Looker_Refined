#!/usr/bin/env python3
"""Test script for the enhanced semantic text-to-SQL system."""

import sys
import os
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.main import TextToSQLEngine

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_enhanced_system():
    """Test the enhanced semantic system with a problematic query."""
    
    print("🧪 Testing Enhanced Semantic Text-to-SQL System")
    print("=" * 60)
    
    # Initialize the engine
    print("Initializing TextToSQL Engine...")
    engine = TextToSQLEngine()
    
    # Test the problematic query from the analysis
    test_query = "Show me revenue by product category"
    
    print(f"\n📊 Testing Query: {test_query}")
    print("Expected: Should use actual transaction values (sale_price) not catalog prices")
    print("-" * 60)
    
    try:
        result = engine.generate_sql(test_query)
        
        if result['error']:
            print(f"❌ Error: {result['error']}")
        else:
            print("✅ SQL Generated:")
            print(result['sql'])
            print(f"\n📈 Metadata:")
            print(f"  - Explore used: {result['explore_used']}")
            print(f"  - LLM used: {result['llm_used']}")
            print(f"  - Processing time: {result['processing_time']:.2f}s")
            
            if result['llm_used']:
                print(f"  - LLM cost: ${result['llm_cost_estimate']:.6f}")
            
            # Check if the SQL uses the correct fields
            sql_lower = result['sql'].lower()
            if 'sale_price' in sql_lower:
                print("\n🎉 SUCCESS: SQL uses actual transaction values (sale_price)")
            elif 'retail_price' in sql_lower or 'product_retail_price' in sql_lower:
                print("\n⚠️  WARNING: SQL uses catalog prices instead of actual revenue")
            else:
                print("\n❓ UNCLEAR: Cannot determine which price field is being used")
                
    except Exception as e:
        print(f"💥 Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enhanced_system()