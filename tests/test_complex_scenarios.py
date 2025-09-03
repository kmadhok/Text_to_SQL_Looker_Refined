"""
Comprehensive test suite for complex LookML Text-to-SQL scenarios.

This module contains challenging test cases that thoroughly exercise the system's
capabilities including complex aggregations, multi-table joins, edge cases,
and error handling scenarios.
"""

import pytest
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum

class TestCategory(Enum):
    """Categories of test cases."""
    COMPLEX_AGGREGATION = "complex_aggregation"
    MULTI_TABLE_JOINS = "multi_table_joins"
    CONDITIONAL_LOGIC = "conditional_logic"
    AMBIGUOUS_QUERIES = "ambiguous_queries"
    PERFORMANCE_SCALE = "performance_scale"
    ERROR_HANDLING = "error_handling"

class DifficultyLevel(Enum):
    """Difficulty levels for test cases."""
    MEDIUM = "medium"
    HARD = "hard"
    EXPERT = "expert"

@dataclass
class TestCase:
    """Represents a single test case with metadata."""
    id: str
    query: str
    category: TestCategory
    difficulty: DifficultyLevel
    description: str
    should_succeed: bool
    expected_behavior: str
    required_explores: List[str]
    required_fields: List[str]
    expected_joins: List[str]
    performance_notes: Optional[str] = None
    edge_case_notes: Optional[str] = None

# Complex Aggregation & Time Series Test Cases
COMPLEX_AGGREGATION_TESTS = [
    TestCase(
        id="agg_001",
        query="Show me the month-over-month growth rate of revenue for each product category, but only for categories that had more than $10k in sales last quarter",
        category=TestCategory.COMPLEX_AGGREGATION,
        difficulty=DifficultyLevel.HARD,
        description="Tests complex time-based aggregation with conditional filtering",
        should_succeed=True,
        expected_behavior="Should generate SQL with LAG() window functions, date filtering, and HAVING clause",
        required_explores=["orders", "order_items", "products"],
        required_fields=["orders.created_date", "order_items.sale_price", "products.category"],
        expected_joins=["orders to order_items", "order_items to products"],
        performance_notes="May be expensive due to window functions and complex date calculations"
    ),
    
    TestCase(
        id="agg_002", 
        query="What's the 7-day rolling average of daily active users, broken down by acquisition channel, for the past 3 months?",
        category=TestCategory.COMPLEX_AGGREGATION,
        difficulty=DifficultyLevel.EXPERT,
        description="Tests rolling window calculations with complex grouping",
        should_succeed=True,
        expected_behavior="Should use window functions with ROWS BETWEEN 6 PRECEDING AND CURRENT ROW",
        required_explores=["events", "users"],
        required_fields=["events.created_date", "users.traffic_source", "users.id"],
        expected_joins=["events to users"],
        performance_notes="Large dataset scan required for 3-month window"
    ),
    
    TestCase(
        id="agg_003",
        query="Find customers whose lifetime value is in the top 10% but whose last purchase was more than 6 months ago",
        category=TestCategory.COMPLEX_AGGREGATION,
        difficulty=DifficultyLevel.HARD,
        description="Tests percentile calculations with temporal conditions",
        should_succeed=True,
        expected_behavior="Should use PERCENTILE_CONT() and MAX() with date filtering",
        required_explores=["users", "orders", "order_items"],
        required_fields=["users.id", "orders.created_date", "order_items.sale_price"],
        expected_joins=["users to orders", "orders to order_items"]
    ),
]

# Multi-Table Join & Relationship Test Cases  
MULTI_TABLE_TESTS = [
    TestCase(
        id="join_001",
        query="Show me the conversion rate from trial to paid subscription for users who signed up through different marketing campaigns, segmented by their company size",
        category=TestCategory.MULTI_TABLE_JOINS,
        difficulty=DifficultyLevel.EXPERT,
        description="Tests complex conversion funnel analysis across multiple dimensions",
        should_succeed=False,  # May not have subscription data in ecommerce model
        expected_behavior="Should attempt to find subscription-related fields, may fail gracefully",
        required_explores=["users", "events"],
        required_fields=["users.traffic_source", "events.event_type"],
        expected_joins=["users to events"],
        edge_case_notes="E-commerce model may not have subscription concepts"
    ),
    
    TestCase(
        id="join_002", 
        query="Which sales reps have the highest close rate for deals over $50k, and what's their average deal cycle time compared to the team average?",
        category=TestCategory.MULTI_TABLE_JOINS,
        difficulty=DifficultyLevel.HARD,
        description="Tests employee performance analysis with complex aggregations",
        should_succeed=False,  # No sales rep data in ecommerce model
        expected_behavior="Should fail to find sales rep related fields",
        required_explores=["orders"],
        required_fields=["orders.sale_price"],
        expected_joins=[],
        edge_case_notes="E-commerce model lacks sales rep hierarchy"
    ),
    
    TestCase(
        id="join_003",
        query="Find products that are frequently bought together, showing the correlation strength and total revenue impact", 
        category=TestCategory.MULTI_TABLE_JOINS,
        difficulty=DifficultyLevel.EXPERT,
        description="Tests market basket analysis with statistical functions",
        should_succeed=True,
        expected_behavior="Should join order_items with itself and calculate co-occurrence statistics",
        required_explores=["order_items", "products"],
        required_fields=["order_items.order_id", "order_items.product_id", "order_items.sale_price"],
        expected_joins=["order_items to products", "self-join on order_items"],
        performance_notes="Complex self-join operation, may require optimization"
    ),
]

# Complex Filtering & Conditional Logic Test Cases
CONDITIONAL_LOGIC_TESTS = [
    TestCase(
        id="cond_001",
        query="Show me revenue trends for customers who either: made their first purchase in Q1 2024 OR have a customer satisfaction score above 8 AND made at least 3 purchases this year",
        category=TestCategory.CONDITIONAL_LOGIC, 
        difficulty=DifficultyLevel.HARD,
        description="Tests complex boolean logic with multiple conditions",
        should_succeed=True,
        expected_behavior="Should generate SQL with complex WHERE clause using OR/AND operators",
        required_explores=["users", "orders", "order_items"],
        required_fields=["orders.created_date", "order_items.sale_price", "users.id"],
        expected_joins=["users to orders", "orders to order_items"],
        edge_case_notes="Customer satisfaction score may not exist in model"
    ),
    
    TestCase(
        id="cond_002",
        query="What percentage of users who abandoned their cart came back to purchase within 48 hours, broken down by cart value ranges?",
        category=TestCategory.CONDITIONAL_LOGIC,
        difficulty=DifficultyLevel.EXPERT, 
        description="Tests complex behavioral analysis with time windows",
        should_succeed=True,
        expected_behavior="Should analyze event sequences and calculate time-based conversion rates",
        required_explores=["events", "users"],
        required_fields=["events.event_type", "events.created_date", "users.id"],
        expected_joins=["events to users"],
        performance_notes="Complex temporal analysis may require window functions"
    ),
]

# Ambiguous & Edge Case Test Cases
AMBIGUOUS_TESTS = [
    TestCase(
        id="amb_001",
        query="Show me the best performing regions",
        category=TestCategory.AMBIGUOUS_QUERIES,
        difficulty=DifficultyLevel.MEDIUM,
        description="Tests handling of ambiguous metrics (best by what measure?)",
        should_succeed=False,
        expected_behavior="Should request clarification or make reasonable assumption about 'best'",
        required_explores=["users", "orders"],
        required_fields=["users.country", "users.state"],
        expected_joins=["users to orders"],
        edge_case_notes="Ambiguous success metric - revenue, users, orders?"
    ),
    
    TestCase(
        id="amb_002",
        query="Give me last month's numbers compared to the same time last year",
        category=TestCategory.AMBIGUOUS_QUERIES,
        difficulty=DifficultyLevel.MEDIUM,
        description="Tests date interpretation and metric inference",
        should_succeed=True,
        expected_behavior="Should interpret relative dates and assume revenue/orders metrics",
        required_explores=["orders", "order_items"],
        required_fields=["orders.created_date", "order_items.sale_price"],
        expected_joins=["orders to order_items"],
        edge_case_notes="Ambiguous which 'numbers' to show"
    ),
    
    TestCase(
        id="amb_003", 
        query="How are we doing with our new customers?",
        category=TestCategory.AMBIGUOUS_QUERIES,
        difficulty=DifficultyLevel.HARD,
        description="Tests extremely vague query interpretation",
        should_succeed=False,
        expected_behavior="Should request significant clarification about metrics and timeframe",
        required_explores=["users", "orders"],
        required_fields=["users.created_date"],
        expected_joins=["users to orders"],
        edge_case_notes="Highly ambiguous - multiple interpretations possible"
    ),
]

# Performance & Scale Test Cases
PERFORMANCE_TESTS = [
    TestCase(
        id="perf_001",
        query="Calculate customer cohort retention rates for every monthly cohort from the past 2 years, showing 12-month retention curves",
        category=TestCategory.PERFORMANCE_SCALE,
        difficulty=DifficultyLevel.EXPERT,
        description="Tests computationally intensive cohort analysis",
        should_succeed=True,
        expected_behavior="Should generate complex SQL with multiple CTEs and window functions",
        required_explores=["users", "orders"],
        required_fields=["users.created_date", "orders.created_date", "users.id"],
        expected_joins=["users to orders"],
        performance_notes="Extremely expensive query - may require query optimization or sampling"
    ),
    
    TestCase(
        id="perf_002",
        query="Show me a heatmap of sales performance by day of week and hour of day for the past year, segmented by product category",
        category=TestCategory.PERFORMANCE_SCALE,
        difficulty=DifficultyLevel.HARD,
        description="Tests large-scale multidimensional aggregation",
        should_succeed=True,
        expected_behavior="Should extract time dimensions and aggregate across multiple axes",
        required_explores=["orders", "order_items", "products"],
        required_fields=["orders.created_date", "order_items.sale_price", "products.category"],
        expected_joins=["orders to order_items", "order_items to products"],
        performance_notes="Large result set with complex grouping dimensions"
    ),
]

# Error Handling Test Cases
ERROR_HANDLING_TESTS = [
    TestCase(
        id="err_001",
        query="Show me revenue for products that don't exist",
        category=TestCategory.ERROR_HANDLING,
        difficulty=DifficultyLevel.MEDIUM,
        description="Tests handling of nonsensical business logic",
        should_succeed=False,
        expected_behavior="Should fail gracefully or return empty result set",
        required_explores=["products", "order_items"],
        required_fields=["products.id", "order_items.sale_price"],
        expected_joins=["products to order_items"],
        edge_case_notes="Logically contradictory request"
    ),
    
    TestCase(
        id="err_002",
        query="Calculate the average of customer names",
        category=TestCategory.ERROR_HANDLING,
        difficulty=DifficultyLevel.MEDIUM,
        description="Tests handling of impossible mathematical operations",
        should_succeed=False,
        expected_behavior="Should reject attempt to aggregate text field mathematically",
        required_explores=["users"],
        required_fields=["users.first_name", "users.last_name"],
        expected_joins=[],
        edge_case_notes="Impossible to calculate average of text values"
    ),
    
    TestCase(
        id="err_003",
        query="Give me data from next year",
        category=TestCategory.ERROR_HANDLING,
        difficulty=DifficultyLevel.MEDIUM,
        description="Tests handling of future date references",
        should_succeed=False,
        expected_behavior="Should either warn about future dates or return empty results",
        required_explores=["orders"],
        required_fields=["orders.created_date"],
        expected_joins=[],
        edge_case_notes="Future data doesn't exist in historical dataset"
    ),
]

# Combine all test cases
ALL_TEST_CASES = (
    COMPLEX_AGGREGATION_TESTS + 
    MULTI_TABLE_TESTS + 
    CONDITIONAL_LOGIC_TESTS + 
    AMBIGUOUS_TESTS + 
    PERFORMANCE_TESTS + 
    ERROR_HANDLING_TESTS
)

def get_test_cases_by_category(category: TestCategory) -> List[TestCase]:
    """Get all test cases for a specific category."""
    return [tc for tc in ALL_TEST_CASES if tc.category == category]

def get_test_cases_by_difficulty(difficulty: DifficultyLevel) -> List[TestCase]:
    """Get all test cases for a specific difficulty level."""
    return [tc for tc in ALL_TEST_CASES if tc.difficulty == difficulty]

def get_expected_success_cases() -> List[TestCase]:
    """Get test cases expected to succeed."""
    return [tc for tc in ALL_TEST_CASES if tc.should_succeed]

def get_expected_failure_cases() -> List[TestCase]:
    """Get test cases expected to fail gracefully."""
    return [tc for tc in ALL_TEST_CASES if not tc.should_succeed]

# Test case summary statistics
def print_test_suite_summary():
    """Print summary statistics about the test suite."""
    print(f"Total test cases: {len(ALL_TEST_CASES)}")
    print(f"Expected to succeed: {len(get_expected_success_cases())}")
    print(f"Expected to fail: {len(get_expected_failure_cases())}")
    
    print("\nBy category:")
    for category in TestCategory:
        count = len(get_test_cases_by_category(category))
        print(f"  {category.value}: {count}")
    
    print("\nBy difficulty:")
    for difficulty in DifficultyLevel:
        count = len(get_test_cases_by_difficulty(difficulty))
        print(f"  {difficulty.value}: {count}")

if __name__ == "__main__":
    print_test_suite_summary()