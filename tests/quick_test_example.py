#!/usr/bin/env python3
"""
Quick test example for LookML Text-to-SQL system.

This script provides a simple example of how to use the testing framework
for basic testing scenarios. It's designed to be self-contained and easy to run.
"""

import sys
import os
from typing import List

# Add parent directory to path to import main engine
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from test_complex_scenarios import TestCase, TestCategory, DifficultyLevel, ALL_TEST_CASES
from validation_utils import validate_test_sql, SQLValidator
from edge_case_generator import EdgeCaseGenerator

def demonstrate_test_cases():
    """Show examples of the predefined test cases."""
    print("PREDEFINED TEST CASES EXAMPLES")
    print("=" * 40)
    
    # Show a few examples from each category
    categories_shown = set()
    
    for test_case in ALL_TEST_CASES:
        if test_case.category not in categories_shown:
            categories_shown.add(test_case.category)
            print(f"\n{test_case.category.value.upper()}: {test_case.id}")
            print(f"Query: {test_case.query}")
            print(f"Difficulty: {test_case.difficulty.value}")
            print(f"Should succeed: {test_case.should_succeed}")
            print(f"Description: {test_case.description}")
            
            if len(categories_shown) >= 3:  # Show just a few examples
                break
    
    print(f"\nTotal predefined test cases: {len(ALL_TEST_CASES)}")
    print()

def demonstrate_sql_validation():
    """Show examples of SQL validation capabilities."""
    print("SQL VALIDATION EXAMPLES")
    print("=" * 25)
    
    validator = SQLValidator()
    
    # Example queries with different characteristics
    test_queries = [
        ("Good query", "SELECT user_id, COUNT(*) as order_count FROM orders WHERE created_date > '2024-01-01' GROUP BY user_id LIMIT 100"),
        ("Missing LIMIT", "SELECT * FROM users WHERE age > 25"),
        ("Potential Cartesian product", "SELECT * FROM users, orders"),
        ("Syntax error", "SELECT COUNT( FROM users"),
        ("Expensive function", "SELECT REGEXP_EXTRACT(name, r'\\w+') FROM users LIMIT 10"),
    ]
    
    for description, query in test_queries:
        print(f"\n{description}:")
        print(f"  SQL: {query[:60]}{'...' if len(query) > 60 else ''}")
        
        is_valid, result = validate_test_sql(query)
        print(f"  Valid: {is_valid}")
        print(f"  Complexity: {result['complexity_metrics']['complexity_level']}")
        print(f"  Issues: {result['error_count']} errors, {result['warning_count']} warnings")
        
        if result['issues']:
            # Show first issue as example
            issue = result['issues'][0]
            print(f"  Example issue: {issue['severity']} - {issue['message']}")
    
    print()

def demonstrate_edge_cases():
    """Show examples of automatically generated edge cases."""
    print("EDGE CASE GENERATION EXAMPLES")  
    print("=" * 32)
    
    generator = EdgeCaseGenerator()
    
    # Generate a few examples of each type
    malformed = generator.generate_malformed_queries(2)
    ambiguous = generator.generate_ambiguous_queries()[:2]
    impossible = generator.generate_impossible_requests()[:2]
    
    print("Malformed Queries:")
    for case in malformed:
        print(f"  '{case.query}' - {case.description}")
    
    print(f"\nAmbiguous Queries:")
    for case in ambiguous:
        print(f"  '{case.query}' - {case.description}")
    
    print(f"\nImpossible Requests:")
    for case in impossible:
        print(f"  '{case.query}' - {case.description}")
    
    # Show total generation capability
    all_edge_cases = generator.generate_all_edge_cases()
    print(f"\nTotal edge cases can be generated: {len(all_edge_cases)}")
    print()

def run_simple_validation_test():
    """Run a simple validation test without the full engine."""
    print("SIMPLE VALIDATION TEST")
    print("=" * 22)
    
    # Test a few representative queries
    simple_tests = [
        "Show me total revenue by product category",
        "Give me top 10 customers by order value", 
        "What's the average order size last month?",
        "Count users who made more than 5 purchases",
        "This is not a valid query at all"
    ]
    
    validator = SQLValidator()
    
    for i, query in enumerate(simple_tests, 1):
        print(f"\nTest {i}: {query}")
        
        # Just validate the query structure and complexity
        # (This doesn't generate SQL, just analyzes the input)
        try:
            # Simple analysis - count words, look for SQL-like patterns
            words = query.lower().split()
            
            has_aggregation = any(word in words for word in ['total', 'count', 'average', 'sum', 'top'])
            has_filtering = any(word in words for word in ['where', 'who', 'last', 'more', 'than'])
            has_grouping = any(word in words for word in ['by', 'per', 'each'])
            
            complexity_indicators = sum([has_aggregation, has_filtering, has_grouping])
            
            if complexity_indicators == 0:
                complexity = "Simple"
            elif complexity_indicators <= 2:
                complexity = "Moderate"  
            else:
                complexity = "Complex"
                
            print(f"  Natural language complexity: {complexity}")
            print(f"  Has aggregation: {has_aggregation}")
            print(f"  Has filtering: {has_filtering}")
            print(f"  Has grouping: {has_grouping}")
            
            # Basic validity check
            is_question_like = query.strip().endswith('?') or any(
                word in words for word in ['show', 'give', 'what', 'how', 'count', 'get']
            )
            print(f"  Appears to be valid query: {is_question_like}")
            
        except Exception as e:
            print(f"  Error analyzing query: {e}")
    
    print()

def print_usage_instructions():
    """Print instructions for running the full test suite."""
    print("NEXT STEPS - RUNNING FULL TESTS")
    print("=" * 32)
    print("This was a quick demonstration. To run the full test suite:")
    print()
    print("1. Demo mode (no actual testing):")
    print("   python run_comprehensive_tests.py --demo")
    print()
    print("2. Quick test run (limited tests):")
    print("   python run_comprehensive_tests.py --quick")
    print()
    print("3. Run specific category:")
    print("   python run_comprehensive_tests.py --category complex_aggregation")
    print()
    print("4. Full test suite:")  
    print("   python run_comprehensive_tests.py")
    print()
    print("5. Include edge cases:")
    print("   python run_comprehensive_tests.py --include-edge-cases")
    print()
    print("Note: Full testing requires proper configuration and may incur costs")
    print("if using LLM-based query planning.")
    print()

def main():
    """Main demonstration function."""
    print("LookML Text-to-SQL Testing Framework")
    print("Quick Example and Demonstration")
    print("=" * 50)
    print()
    
    try:
        # Run demonstrations
        demonstrate_test_cases()
        demonstrate_sql_validation() 
        demonstrate_edge_cases()
        run_simple_validation_test()
        print_usage_instructions()
        
        print("Quick demonstration completed successfully!")
        
    except Exception as e:
        print(f"Error during demonstration: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)