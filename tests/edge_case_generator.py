"""
Edge case generator and error handling test utilities.

This module provides utilities for generating edge cases and testing
error handling scenarios in the LookML Text-to-SQL system.
"""

import random
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

from test_complex_scenarios import TestCase, TestCategory, DifficultyLevel

class EdgeCaseType(Enum):
    """Types of edge cases to generate."""
    MALFORMED_QUERY = "malformed_query"
    AMBIGUOUS_INTENT = "ambiguous_intent" 
    IMPOSSIBLE_REQUEST = "impossible_request"
    EXTREME_VALUES = "extreme_values"
    TEMPORAL_EDGE_CASES = "temporal_edge_cases"
    UNICODE_SPECIAL_CHARS = "unicode_special_chars"
    INJECTION_ATTEMPTS = "injection_attempts"
    VERY_LONG_QUERIES = "very_long_queries"
    NONSENSICAL_COMBINATIONS = "nonsensical_combinations"

@dataclass
class GeneratedEdgeCase:
    """An automatically generated edge case."""
    query: str
    edge_case_type: EdgeCaseType
    description: str
    expected_behavior: str
    should_succeed: bool
    test_purpose: str

class EdgeCaseGenerator:
    """Generates edge cases for comprehensive testing."""
    
    def __init__(self):
        """Initialize the edge case generator."""
        self.base_fields = [
            "revenue", "users", "orders", "products", "categories", 
            "customers", "sales", "profit", "quantity", "price"
        ]
        
        self.time_expressions = [
            "yesterday", "last week", "last month", "last quarter", "last year",
            "today", "this week", "this month", "this quarter", "this year",
            "next week", "next month", "2025", "January 2024", "Q1 2023"
        ]
        
        self.malformed_patterns = [
            "Show me the {} for",  # Incomplete
            "Give me {} where",    # Incomplete
            "How many {} are there in the (",  # Unmatched paren
            "What is the average of {} and",   # Incomplete
            "{} by {} from",       # Incomplete
        ]
    
    def generate_malformed_queries(self, count: int = 5) -> List[GeneratedEdgeCase]:
        """Generate malformed or incomplete queries."""
        edge_cases = []
        
        for _ in range(count):
            pattern = random.choice(self.malformed_patterns)
            field = random.choice(self.base_fields)
            query = pattern.format(field)
            
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.MALFORMED_QUERY,
                description=f"Incomplete query: {pattern}",
                expected_behavior="Should handle gracefully and request clarification",
                should_succeed=False,
                test_purpose="Test parser robustness with incomplete inputs"
            ))
        
        return edge_cases
    
    def generate_ambiguous_queries(self) -> List[GeneratedEdgeCase]:
        """Generate highly ambiguous queries."""
        ambiguous_queries = [
            "Show me the numbers",
            "How are we doing?",
            "Give me the data",
            "What's the performance?", 
            "Show me everything",
            "How much did we make?",
            "What's trending?",
            "Give me the metrics",
            "Show me the best ones",
            "What changed?"
        ]
        
        edge_cases = []
        for query in ambiguous_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.AMBIGUOUS_INTENT,
                description=f"Highly ambiguous query with no specific metrics or timeframe",
                expected_behavior="Should request clarification about specific metrics and filters",
                should_succeed=False,
                test_purpose="Test natural language understanding limits"
            ))
        
        return edge_cases
    
    def generate_impossible_requests(self) -> List[GeneratedEdgeCase]:
        """Generate logically impossible or contradictory requests."""
        impossible_queries = [
            "Show me users who never existed",
            "Calculate the square root of customer names", 
            "Give me negative positive numbers",
            "Show me products sold before they were created",
            "Count the average of text fields",
            "Sum all the colors",
            "Show me users from the future",
            "Calculate revenue for imaginary products",
            "Give me the median customer name",
            "Show me orders that were never placed"
        ]
        
        edge_cases = []
        for query in impossible_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.IMPOSSIBLE_REQUEST,
                description="Logically impossible or contradictory request",
                expected_behavior="Should reject gracefully with explanation",
                should_succeed=False,
                test_purpose="Test logical validation and error handling"
            ))
        
        return edge_cases
    
    def generate_extreme_value_queries(self) -> List[GeneratedEdgeCase]:
        """Generate queries with extreme values."""
        extreme_queries = [
            "Show me top 999999999 customers",
            "Give me revenue for year 1",
            "Show me products with price > 99999999999",
            "Count users born in year 3000",
            "Show me orders with -500 quantity",
            "Give me revenue between 0 and 999999999999999",
            "Show me top 0 products",  # Zero limit
            "Give me bottom -10 customers",  # Negative limit
        ]
        
        edge_cases = []
        for query in extreme_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.EXTREME_VALUES,
                description="Query with extreme or edge-case numeric values",
                expected_behavior="Should handle gracefully, may return empty results or apply sensible limits",
                should_succeed=True,  # Should handle but may return empty results
                test_purpose="Test numeric boundary handling"
            ))
        
        return edge_cases
    
    def generate_temporal_edge_cases(self) -> List[GeneratedEdgeCase]:
        """Generate temporal edge cases."""
        temporal_queries = [
            "Show me revenue for February 30th",  # Invalid date
            "Give me data from next century",
            "Show me orders placed at 25:00",  # Invalid time
            "Count users from year 0",
            "Give me sales for the 13th month",
            "Show me data from before time began",
            "Count orders placed on Nonday",  # Invalid day
            "Show me future historical data",  # Contradiction
        ]
        
        edge_cases = []
        for query in temporal_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.TEMPORAL_EDGE_CASES,
                description="Query with invalid or edge-case temporal references",
                expected_behavior="Should handle invalid dates gracefully or return empty results",
                should_succeed=False,
                test_purpose="Test temporal parsing robustness"
            ))
        
        return edge_cases
    
    def generate_unicode_queries(self) -> List[GeneratedEdgeCase]:
        """Generate queries with special characters and unicode."""
        unicode_queries = [
            "Show me revenue for café products",  # Accent
            "Count users in москва region",  # Cyrillic  
            "Give me sales for 北京 office",  # Chinese
            "Show me products with emoji 🎉 names",  # Emoji
            "Count orders from ñoño customers",  # Special characters
            'Show me "revenue" with quotes',  # Quotes
            "Give me data for <script>alert('test')</script>",  # HTML injection attempt
            "Show me users\\nwith\\nnewlines",  # Escape characters
        ]
        
        edge_cases = []
        for query in unicode_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.UNICODE_SPECIAL_CHARS,
                description="Query with unicode, special characters, or encoding issues",
                expected_behavior="Should handle unicode gracefully, sanitize potential injection",
                should_succeed=True,  # Should handle but may need sanitization
                test_purpose="Test character encoding and injection prevention"
            ))
        
        return edge_cases
    
    def generate_injection_attempts(self) -> List[GeneratedEdgeCase]:
        """Generate SQL injection and other malicious attempts."""
        injection_queries = [
            "Show me users'; DROP TABLE users; --",  # SQL injection
            "Give me revenue OR 1=1",  # SQL injection
            "Show me users UNION SELECT * FROM sensitive_table",  # Union injection
            "Count orders WHERE 1=1; DELETE FROM orders",  # Multiple statements
            'Show me products"; system("rm -rf /"); --',  # Command injection
            "Give me data {{constructor.constructor('return process')().exit()}}",  # Template injection
            "Show me users <script>alert('xss')</script>",  # XSS attempt
            "Count /* SQL COMMENT */ users",  # Comment injection
        ]
        
        edge_cases = []
        for query in injection_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.INJECTION_ATTEMPTS,
                description="Query attempting various injection attacks",
                expected_behavior="Should sanitize input and prevent malicious code execution",
                should_succeed=False,
                test_purpose="Test security against injection attacks"
            ))
        
        return edge_cases
    
    def generate_very_long_queries(self) -> List[GeneratedEdgeCase]:
        """Generate extremely long queries."""
        # Generate a very long field list
        long_field_list = ", ".join([f"field_{i}" for i in range(100)])
        
        # Generate very long WHERE clause
        long_where = " AND ".join([f"field_{i} > {i}" for i in range(50)])
        
        long_queries = [
            f"Show me {long_field_list} from users",
            f"Give me revenue where {long_where}",
            "Show me " + "very " * 100 + "long query",
            "A" * 10000,  # Extremely long single token
        ]
        
        edge_cases = []
        for query in long_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.VERY_LONG_QUERIES,
                description=f"Extremely long query ({len(query)} characters)",
                expected_behavior="Should handle long input gracefully, may truncate or reject",
                should_succeed=False,
                test_purpose="Test input length limits and memory handling"
            ))
        
        return edge_cases
    
    def generate_nonsensical_combinations(self) -> List[GeneratedEdgeCase]:
        """Generate queries with nonsensical field/operation combinations."""
        nonsensical_queries = [
            "Show me the maximum minimum value",  # Contradictory
            "Count the average of unique distinct items",  # Redundant/contradictory
            "Sum all the text fields alphabetically",  # Wrong operation for data type
            "Sort by color numerically",  # Wrong sort type
            "Group by aggregated values",  # Circular logic
            "Filter after limiting after filtering",  # Redundant operations  
            "Show me the total count of averages",  # Nested contradictions
            "Give me distinct duplicate values",  # Contradictory
        ]
        
        edge_cases = []
        for query in nonsensical_queries:
            edge_cases.append(GeneratedEdgeCase(
                query=query,
                edge_case_type=EdgeCaseType.NONSENSICAL_COMBINATIONS,
                description="Query with nonsensical combinations of operations",
                expected_behavior="Should detect logical inconsistencies and request clarification",
                should_succeed=False,
                test_purpose="Test logical validation of operation combinations"
            ))
        
        return edge_cases
    
    def generate_all_edge_cases(self) -> List[GeneratedEdgeCase]:
        """Generate all types of edge cases."""
        all_edge_cases = []
        
        all_edge_cases.extend(self.generate_malformed_queries(3))
        all_edge_cases.extend(self.generate_ambiguous_queries())
        all_edge_cases.extend(self.generate_impossible_requests())
        all_edge_cases.extend(self.generate_extreme_value_queries())
        all_edge_cases.extend(self.generate_temporal_edge_cases())
        all_edge_cases.extend(self.generate_unicode_queries())
        all_edge_cases.extend(self.generate_injection_attempts())
        all_edge_cases.extend(self.generate_very_long_queries())
        all_edge_cases.extend(self.generate_nonsensical_combinations())
        
        return all_edge_cases
    
    def convert_to_test_cases(self, generated_cases: List[GeneratedEdgeCase]) -> List[TestCase]:
        """Convert generated edge cases to TestCase format."""
        test_cases = []
        
        for i, edge_case in enumerate(generated_cases):
            test_case = TestCase(
                id=f"edge_{edge_case.edge_case_type.value}_{i+1:02d}",
                query=edge_case.query,
                category=TestCategory.ERROR_HANDLING,
                difficulty=DifficultyLevel.MEDIUM,
                description=edge_case.description,
                should_succeed=edge_case.should_succeed,
                expected_behavior=edge_case.expected_behavior,
                required_explores=[],  # Edge cases typically don't require specific explores
                required_fields=[],
                expected_joins=[],
                edge_case_notes=edge_case.test_purpose
            )
            test_cases.append(test_case)
        
        return test_cases

class ErrorSimulator:
    """Simulates various error conditions for testing."""
    
    @staticmethod
    def simulate_timeout_scenario(timeout_seconds: int = 30) -> Dict[str, Any]:
        """Simulate a query timeout scenario."""
        return {
            "type": "timeout_simulation",
            "timeout_seconds": timeout_seconds,
            "expected_behavior": "Should handle timeout gracefully and return partial results or error",
            "test_query": "Show me very complex aggregation that might timeout"
        }
    
    @staticmethod
    def simulate_memory_pressure() -> Dict[str, Any]:
        """Simulate memory pressure scenarios."""
        return {
            "type": "memory_pressure",
            "expected_behavior": "Should handle memory constraints gracefully",
            "test_query": "Generate query plan for extremely complex multi-table joins"
        }
    
    @staticmethod
    def simulate_llm_service_failure() -> Dict[str, Any]:
        """Simulate LLM service failures."""
        return {
            "type": "llm_service_failure", 
            "expected_behavior": "Should fallback to rule-based planner or fail gracefully",
            "test_scenario": "Mock LLM service to return errors or timeouts"
        }
    
    @staticmethod
    def simulate_bigquery_quota_exceeded() -> Dict[str, Any]:
        """Simulate BigQuery quota exceeded scenarios."""
        return {
            "type": "quota_exceeded",
            "expected_behavior": "Should handle quota limits gracefully with informative error",
            "test_scenario": "Mock BigQuery client to return quota exceeded errors"
        }

def generate_comprehensive_edge_case_suite() -> List[TestCase]:
    """Generate a comprehensive suite of edge case tests."""
    generator = EdgeCaseGenerator()
    generated_cases = generator.generate_all_edge_cases()
    return generator.convert_to_test_cases(generated_cases)

if __name__ == "__main__":
    # Generate and print summary of edge cases
    generator = EdgeCaseGenerator()
    all_cases = generator.generate_all_edge_cases()
    
    print(f"Generated {len(all_cases)} edge cases:")
    
    by_type = {}
    for case in all_cases:
        case_type = case.edge_case_type.value
        if case_type not in by_type:
            by_type[case_type] = 0
        by_type[case_type] += 1
    
    for case_type, count in by_type.items():
        print(f"  {case_type}: {count}")
    
    # Show some examples
    print("\nExample edge cases:")
    for case in all_cases[:5]:
        print(f"  - {case.query} ({case.edge_case_type.value})")
    
    # Convert to test cases
    test_cases = generator.convert_to_test_cases(all_cases)
    print(f"\nConverted to {len(test_cases)} TestCase objects")