#!/usr/bin/env python3
"""
Comprehensive test runner for LookML Text-to-SQL system.

This script demonstrates how to use the complete testing framework including:
- Complex scenario testing
- Edge case generation and testing
- SQL validation and performance analysis
- Automated reporting and analysis

Usage examples:
    # Run all tests
    python run_comprehensive_tests.py

    # Run only specific categories
    python run_comprehensive_tests.py --category complex_aggregation --category multi_table_joins

    # Run with custom configuration
    python run_comprehensive_tests.py --config config/config.yaml --output-dir ./test_results

    # Quick test run (limited test count)
    python run_comprehensive_tests.py --quick --max-tests 10

    # Generate edge cases only
    python run_comprehensive_tests.py --edge-cases-only
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Add tests directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'tests'))

from tests.test_complex_scenarios import (
    ALL_TEST_CASES, TestCategory, DifficultyLevel,
    get_test_cases_by_category, get_test_cases_by_difficulty
)
from tests.test_runner import ComplexScenarioTestRunner
from tests.edge_case_generator import (
    EdgeCaseGenerator, generate_comprehensive_edge_case_suite
)
from tests.validation_utils import SQLValidator, validate_test_sql

def print_banner():
    """Print welcome banner."""
    print("=" * 70)
    print("LookML Text-to-SQL Comprehensive Test Suite")
    print("=" * 70)
    print(f"Starting test run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

def print_test_categories_summary():
    """Print summary of available test categories."""
    print("AVAILABLE TEST CATEGORIES:")
    print("-" * 30)
    
    for category in TestCategory:
        count = len(get_test_cases_by_category(category))
        print(f"  {category.value}: {count} tests")
    
    print(f"\nTotal predefined tests: {len(ALL_TEST_CASES)}")
    print()

def print_difficulty_summary():
    """Print summary by difficulty level."""
    print("TESTS BY DIFFICULTY LEVEL:")
    print("-" * 30)
    
    for difficulty in DifficultyLevel:
        count = len(get_test_cases_by_difficulty(difficulty))
        print(f"  {difficulty.value}: {count} tests")
    print()

def run_validation_examples():
    """Run some validation examples to demonstrate the validation framework."""
    print("RUNNING VALIDATION EXAMPLES:")
    print("-" * 30)
    
    validator = SQLValidator()
    
    # Example queries for validation
    example_queries = [
        "SELECT * FROM users LIMIT 100",  # Good query
        "SELECT user_id, SUM(revenue) FROM orders GROUP BY user_id",  # Missing LIMIT
        "SELECT * FROM users, orders",  # Potential Cartesian product
        "SELECT COUNT( FROM users",  # Syntax error
        "SELECT REGEXP_EXTRACT(name, r'pattern') FROM users",  # Expensive function
    ]
    
    for i, query in enumerate(example_queries, 1):
        print(f"\nExample {i}: {query}")
        is_valid, validation_result = validate_test_sql(query)
        
        print(f"  Valid: {is_valid}")
        print(f"  Issues found: {validation_result['error_count']} errors, "
              f"{validation_result['warning_count']} warnings")
        
        if validation_result['issues']:
            for issue in validation_result['issues'][:2]:  # Show first 2 issues
                print(f"    - {issue['severity']}: {issue['message']}")
        
        complexity = validation_result['complexity_metrics']
        print(f"  Complexity: {complexity['complexity_level']} "
              f"(score: {complexity['complexity_score']:.1f})")
    
    print()

def generate_and_show_edge_cases():
    """Generate and display some edge cases."""
    print("GENERATING EDGE CASES:")
    print("-" * 22)
    
    generator = EdgeCaseGenerator()
    
    # Generate a few examples from each type
    malformed = generator.generate_malformed_queries(2)
    ambiguous = generator.generate_ambiguous_queries()[:3]
    impossible = generator.generate_impossible_requests()[:3]
    
    print("Malformed Queries:")
    for case in malformed:
        print(f"  - '{case.query}' ({case.description})")
    
    print("\nAmbiguous Queries:")
    for case in ambiguous:
        print(f"  - '{case.query}' ({case.description})")
    
    print("\nImpossible Requests:")
    for case in impossible:
        print(f"  - '{case.query}' ({case.description})")
    
    # Show total count
    all_edge_cases = generator.generate_all_edge_cases()
    print(f"\nTotal edge cases generated: {len(all_edge_cases)}")
    print()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Comprehensive test runner for LookML Text-to-SQL system",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Configuration options
    parser.add_argument("--config", "-c", help="Path to configuration file")
    parser.add_argument("--output-dir", default="test_results", 
                       help="Output directory for results (default: test_results)")
    
    # Test selection options
    parser.add_argument("--category", choices=[c.value for c in TestCategory], 
                       action="append", help="Filter by category (can specify multiple)")
    parser.add_argument("--difficulty", choices=[d.value for d in DifficultyLevel],
                       action="append", help="Filter by difficulty (can specify multiple)")
    parser.add_argument("--test-id", action="append", help="Run specific test IDs")
    parser.add_argument("--max-tests", type=int, help="Maximum number of tests to run")
    
    # Test execution options
    parser.add_argument("--no-validation", action="store_true", 
                       help="Disable SQL validation")
    parser.add_argument("--no-performance", action="store_true",
                       help="Disable performance tracking")
    parser.add_argument("--include-edge-cases", action="store_true",
                       help="Include automatically generated edge cases")
    parser.add_argument("--edge-cases-only", action="store_true",
                       help="Run only edge cases")
    
    # Convenience options
    parser.add_argument("--quick", action="store_true",
                       help="Quick test run (limits tests and disables some features)")
    parser.add_argument("--demo", action="store_true",
                       help="Demo mode - shows examples without running full tests")
    parser.add_argument("--list-tests", action="store_true",
                       help="List all available tests and exit")
    parser.add_argument("--validate-only", action="store_true",
                       help="Run validation examples only")
    
    args = parser.parse_args()
    
    # Print banner
    print_banner()
    
    # Demo mode - show examples and capabilities
    if args.demo:
        print_test_categories_summary()
        print_difficulty_summary()
        run_validation_examples()
        generate_and_show_edge_cases()
        print("Demo completed. Use without --demo to run actual tests.")
        return
    
    # List tests mode
    if args.list_tests:
        print_test_categories_summary()
        print_difficulty_summary()
        
        print("PREDEFINED TEST CASES:")
        print("-" * 22)
        for test_case in ALL_TEST_CASES[:10]:  # Show first 10
            print(f"  {test_case.id}: {test_case.query[:60]}...")
        
        if len(ALL_TEST_CASES) > 10:
            print(f"  ... and {len(ALL_TEST_CASES) - 10} more")
        return
    
    # Validation only mode
    if args.validate_only:
        run_validation_examples()
        return
    
    # Configure for quick mode
    if args.quick:
        if not args.max_tests:
            args.max_tests = 10
        args.no_performance = True
        print("Quick mode enabled: limited tests, performance tracking disabled\n")
    
    # Convert string arguments to enums
    categories = None
    if args.category:
        categories = [TestCategory(c) for c in args.category]
    
    difficulties = None
    if args.difficulty:
        difficulties = [DifficultyLevel(d) for d in args.difficulty]
    
    # Prepare test cases
    test_cases = []
    
    if not args.edge_cases_only:
        # Add predefined test cases
        if categories or difficulties or args.test_id:
            # Filtered selection
            filtered_cases = ALL_TEST_CASES
            
            if categories:
                filtered_cases = [tc for tc in filtered_cases if tc.category in categories]
            if difficulties:
                filtered_cases = [tc for tc in filtered_cases if tc.difficulty in difficulties]
            if args.test_id:
                filtered_cases = [tc for tc in filtered_cases if tc.id in args.test_id]
            
            test_cases.extend(filtered_cases)
        else:
            # All predefined cases
            test_cases.extend(ALL_TEST_CASES)
    
    # Add edge cases if requested
    if args.include_edge_cases or args.edge_cases_only:
        edge_cases = generate_comprehensive_edge_case_suite()
        test_cases.extend(edge_cases)
        print(f"Added {len(edge_cases)} automatically generated edge cases")
    
    # Apply max_tests limit
    if args.max_tests and len(test_cases) > args.max_tests:
        original_count = len(test_cases)
        test_cases = test_cases[:args.max_tests]
        print(f"Limited to {args.max_tests} tests (from {original_count} total)")
    
    print(f"Running {len(test_cases)} test cases")
    print()
    
    # Create and configure test runner
    runner = ComplexScenarioTestRunner(
        config_path=args.config,
        output_dir=args.output_dir,
        enable_validation=not args.no_validation,
        enable_performance_tracking=not args.no_performance
    )
    
    try:
        # Run tests
        print("Starting test execution...")
        results = runner.run_test_suite(
            categories=categories,
            difficulties=difficulties,
            test_ids=args.test_id,
            max_tests=args.max_tests
        )
        
        # Print final summary
        print("\n" + "=" * 70)
        print("FINAL RESULTS SUMMARY")
        print("=" * 70)
        print(f"Tests completed: {results.total_tests}")
        print(f"Success rate: {results.success_rate:.1f}%")
        print(f"Expected success rate: {results.expected_success_rate:.1f}%")
        print(f"Unexpected failures: {results.unexpected_failures}")
        print(f"Total execution time: {results.total_execution_time:.2f}s")
        print(f"Total LLM cost: ${results.total_llm_cost:.4f}")
        
        # Performance insights
        avg_time = results.total_execution_time / results.total_tests
        print(f"Average time per test: {avg_time:.2f}s")
        
        if results.total_llm_cost > 0:
            avg_cost = results.total_llm_cost / results.total_tests
            print(f"Average LLM cost per test: ${avg_cost:.6f}")
        
        # Results by category
        print(f"\nResults by category:")
        for category, stats in results.results_by_category.items():
            success_rate = (stats['passed'] / stats['total']) * 100
            print(f"  {category}: {success_rate:.1f}% ({stats['passed']}/{stats['total']})")
        
        # Exit code based on results
        if results.unexpected_failures > 0:
            print(f"\nWARNING: {results.unexpected_failures} unexpected test failures")
            print("Check the detailed logs for more information")
            exit_code = 1
        else:
            print("\nAll tests completed as expected!")
            exit_code = 0
        
        print(f"\nDetailed results saved to: {args.output_dir}/")
        
    except KeyboardInterrupt:
        print("\nTest execution interrupted by user")
        exit_code = 2
    except Exception as e:
        print(f"\nTest execution failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 3
    
    exit(exit_code)

if __name__ == "__main__":
    main()