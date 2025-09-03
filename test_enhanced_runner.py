#!/usr/bin/env python3
"""
Test script to demonstrate the enhanced test runner with LLM conversation capture.

This shows you exactly what data is now being captured compared to before.
"""

import json
from pathlib import Path

def demonstrate_enhanced_capture():
    """Show what the enhanced test runner now captures."""
    
    print("ENHANCED TEST RUNNER DEMONSTRATION")
    print("=" * 40)
    print()
    
    print("🎯 WHAT WE'RE NOW CAPTURING:")
    print("-" * 30)
    print("✅ Original data (from before):")
    print("  - Question asked")
    print("  - SQL generated") 
    print("  - Success/failure status")
    print("  - LLM cost and token usage")
    print("  - Execution time")
    print("  - Explore and fields used")
    print()
    
    print("✅ NEW: LLM Conversation Details:")
    print("  - llm_prompt_sent: Full prompt sent to LLM")
    print("  - llm_system_prompt: System instructions")
    print("  - llm_schema_context: Schema info provided")
    print("  - llm_raw_response: Raw LLM response")
    print("  - llm_conversation_id: Unique conversation ID")
    print()
    
    print("📊 HOW IT WORKS:")
    print("-" * 20)
    print("1. Test runner calls your TextToSQLEngine")
    print("2. Engine uses LLMQueryPlanner → GeminiService")
    print("3. GeminiService saves conversation logs to files")
    print("4. Test runner reads the most recent log file")
    print("5. Extracts prompt/response data into test results")
    print()
    
    print("📁 SAMPLE OUTPUT STRUCTURE:")
    print("-" * 30)
    
    sample_output = {
        "question": "Show me total revenue",
        "success": True,
        "sql_output": "SELECT SUM(sale_price) FROM order_items LIMIT 100",
        "llm_used": True,
        "llm_cost": 0.00933,
        "llm_tokens": {"prompt_tokens": 2328, "completion_tokens": 40},
        
        # NEW FIELDS
        "llm_prompt_sent": "Database Schema:\n...\nNatural Language Query: Show me total revenue\nGenerate the BigQuery SQL query:",
        "llm_system_prompt": "You are an expert SQL developer specializing in BigQuery...",
        "llm_schema_context": "# Available Tables and Fields\n## order_items\n- sale_price (FLOAT): Actual sale price...",
        "llm_raw_response": "SELECT\n  sum(t1.sale_price) AS total_revenue\nFROM\n  `bigquery-public-data.thelook_ecommerce.order_items` AS t1\nLIMIT 100",
        "llm_conversation_id": "1f973993_attempt_1",
        
        "execution_time": 7.65,
        "explore_used": "thelook_ecommerce.model.order_items"
    }
    
    # Pretty print the sample
    print(json.dumps(sample_output, indent=2))
    print()
    
    print("🔍 ANALYSIS CAPABILITIES:")
    print("-" * 25)
    print("With this data, you can now analyze:")
    print("• What schema context is being sent to the LLM")
    print("• How the LLM interprets different question types")
    print("• Prompt size vs. response quality patterns")
    print("• Which schema info leads to better SQL generation")
    print("• LLM reasoning patterns and failure modes")
    print()
    
    print("🚀 USAGE:")
    print("-" * 10)
    print("# Run enhanced tests")
    print("python simple_test_runner.py")
    print()
    print("# Analyze with LLM conversation details")
    print("python analyze_test_results.py test_results_20250903_123456.json")
    print()
    print("# Show more LLM conversation examples")
    print("python analyze_test_results.py test_results_20250903_123456.json --llm-conversations 5")
    print()
    
    print("💡 INSIGHTS YOU'LL GET:")
    print("-" * 22)
    print("• 'The LLM is getting confused by ambiguous field names'")
    print("• 'Queries about products need better schema context'") 
    print("• 'Long prompts (>5000 chars) have worse success rates'")
    print("• 'The system prompt needs adjustment for time-based queries'")
    print("• 'Schema context is missing key relationship information'")
    print()
    
    print("🔧 REQUIREMENTS:")
    print("-" * 15)
    print("For this to work, make sure:")
    print("• Your config has save_conversations: true")
    print("• conversation_log_dir is set (e.g., 'data/llm_logs')")
    print("• The LLM planner is enabled (use_llm_planner: true)")
    print()
    
    # Check if we can find any existing results to compare
    existing_results = list(Path('.').glob('test_results_*.json'))
    if existing_results:
        latest = max(existing_results, key=lambda p: p.stat().st_mtime)
        print(f"📄 Found existing test results: {latest}")
        print("Run the analyzer on this file to see current data capture level")
    else:
        print("📄 No existing test results found")
        print("Run python simple_test_runner.py to generate test data")

if __name__ == "__main__":
    demonstrate_enhanced_capture()