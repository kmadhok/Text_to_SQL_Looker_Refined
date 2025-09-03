# LookML Text-to-SQL Comprehensive Testing Framework

This directory contains a comprehensive testing framework for the LookML Text-to-SQL system. The framework provides sophisticated testing capabilities including complex scenario testing, edge case generation, SQL validation, and performance benchmarking.

## Overview

The testing framework consists of several key components:

- **Complex Scenario Tests** (`test_complex_scenarios.py`) - 16 carefully crafted test cases covering various complexity levels
- **Automated Test Runner** (`test_runner.py`) - Comprehensive test execution engine with reporting
- **SQL Validation** (`validation_utils.py`) - Advanced SQL analysis and validation utilities
- **Edge Case Generator** (`edge_case_generator.py`) - Automatic generation of edge cases and error scenarios
- **Main Test Script** (`../run_comprehensive_tests.py`) - Primary entry point for running tests

## Quick Start

### Basic Usage

```bash
# Run all tests with default configuration
python run_comprehensive_tests.py

# Demo mode - see examples without running tests
python run_comprehensive_tests.py --demo

# Quick test run (limited tests)
python run_comprehensive_tests.py --quick

# List available tests
python run_comprehensive_tests.py --list-tests
```

### Running Specific Test Categories

```bash
# Run only complex aggregation tests
python run_comprehensive_tests.py --category complex_aggregation

# Run multiple categories
python run_comprehensive_tests.py --category complex_aggregation --category multi_table_joins

# Run by difficulty level
python run_comprehensive_tests.py --difficulty hard --difficulty expert

# Run specific test IDs
python run_comprehensive_tests.py --test-id agg_001 --test-id join_002
```

### Edge Cases and Advanced Testing

```bash
# Include automatically generated edge cases
python run_comprehensive_tests.py --include-edge-cases

# Run only edge cases
python run_comprehensive_tests.py --edge-cases-only

# Validation examples only
python run_comprehensive_tests.py --validate-only
```

### Configuration Options

```bash
# Use custom configuration
python run_comprehensive_tests.py --config path/to/config.yaml

# Custom output directory
python run_comprehensive_tests.py --output-dir ./my_test_results

# Disable SQL validation (faster execution)
python run_comprehensive_tests.py --no-validation

# Limit number of tests
python run_comprehensive_tests.py --max-tests 10
```

## Test Categories

### 1. Complex Aggregation Tests (`complex_aggregation`)
Tests advanced aggregation scenarios including:
- Month-over-month growth calculations
- Rolling averages and window functions
- Percentile calculations with temporal conditions

**Examples:**
- "Show me the month-over-month growth rate of revenue for each product category"
- "What's the 7-day rolling average of daily active users, broken down by acquisition channel?"

### 2. Multi-Table Joins (`multi_table_joins`)
Tests complex relationship queries:
- Multi-table join scenarios
- Market basket analysis
- Performance analysis across entities

**Examples:**
- "Find products that are frequently bought together, showing correlation strength"
- "Show me conversion rates segmented by company size"

### 3. Conditional Logic (`conditional_logic`)
Tests complex boolean logic and conditions:
- Complex WHERE clauses with multiple conditions
- Behavioral analysis with time windows
- Nested conditional logic

**Examples:**
- "Show me users who either made their first purchase in Q1 2024 OR have satisfaction score > 8"
- "What percentage of users who abandoned cart came back within 48 hours?"

### 4. Ambiguous Queries (`ambiguous_queries`)
Tests natural language understanding limits:
- Vague requests requiring clarification
- Ambiguous metrics and timeframes
- Intent disambiguation

**Examples:**
- "Show me the best performing regions" (best by what measure?)
- "How are we doing with our new customers?" (multiple interpretations)

### 5. Performance Scale (`performance_scale`)
Tests computationally intensive scenarios:
- Large-scale cohort analysis
- Complex multidimensional aggregations
- Resource-intensive calculations

**Examples:**
- "Calculate customer cohort retention rates for every monthly cohort from past 2 years"
- "Show me a heatmap of sales by day of week and hour for the past year"

### 6. Error Handling (`error_handling`)
Tests robustness and error handling:
- Logically impossible requests
- Invalid mathematical operations
- Future date references

**Examples:**
- "Calculate the average of customer names" (impossible operation)
- "Give me data from next year" (future data doesn't exist)

## Difficulty Levels

- **Medium**: Standard complexity queries that should generally work
- **Hard**: Complex queries requiring advanced SQL features
- **Expert**: Very complex scenarios pushing system limits

## Edge Case Generation

The framework includes automatic edge case generation for:

- **Malformed Queries**: Incomplete or syntactically invalid inputs
- **Injection Attempts**: SQL injection and security testing
- **Unicode/Special Characters**: Character encoding and sanitization
- **Extreme Values**: Boundary conditions and edge numbers
- **Temporal Edge Cases**: Invalid dates and time references

## SQL Validation Features

The validation system provides:

### Syntax Validation
- Parentheses matching
- Complete statement validation  
- Common syntax error detection

### Performance Analysis
- Missing LIMIT clause detection
- Cartesian product identification
- Expensive function usage analysis
- Query complexity scoring

### Best Practices
- Table aliasing recommendations
- Naming convention validation
- BigQuery-specific optimizations

### Complexity Metrics
- Table and join counting
- Nested subquery depth analysis
- Function usage categorization
- Overall complexity scoring

## Output and Reporting

The test runner generates comprehensive reports:

### JSON Results
Detailed machine-readable results including:
- Individual test outcomes
- Performance metrics
- LLM cost tracking
- Validation results

### Summary Reports  
Human-readable summaries with:
- Success rates by category/difficulty
- Performance analysis
- Cost estimation
- Failure analysis

### Logs
Detailed execution logs for debugging:
- Test execution progress
- Error messages and stack traces
- Performance timing data

## Example Test Results Structure

```json
{
  "timestamp": "2024-01-15T10:30:00",
  "total_tests": 25,
  "passed_tests": 18,
  "failed_tests": 7,
  "expected_failures": 5,
  "unexpected_failures": 2,
  "total_execution_time": 45.2,
  "total_llm_cost": 0.0245,
  "results_by_category": {
    "complex_aggregation": {"total": 3, "passed": 3, "failed": 0},
    "error_handling": {"total": 8, "passed": 3, "failed": 5}
  }
}
```

## Integration with CI/CD

The test runner returns appropriate exit codes:
- `0`: All tests passed as expected
- `1`: Unexpected test failures occurred  
- `2`: Test execution interrupted
- `3`: Test execution failed with error

Example CI integration:
```yaml
# .github/workflows/test.yml
- name: Run comprehensive tests
  run: python run_comprehensive_tests.py --quick --no-performance
  continue-on-error: false
```

## Customization and Extension

### Adding Custom Test Cases

1. Define test cases in `test_complex_scenarios.py`:
```python
new_test = TestCase(
    id="custom_001",
    query="Your natural language query",
    category=TestCategory.COMPLEX_AGGREGATION,
    difficulty=DifficultyLevel.HARD,
    description="Description of what this tests",
    should_succeed=True,
    expected_behavior="What should happen",
    required_explores=["orders", "users"],
    required_fields=["orders.created_date"],
    expected_joins=["orders to users"]
)
```

2. Add to `ALL_TEST_CASES` list

### Custom Validation Rules

Extend `SQLValidator` class in `validation_utils.py`:
```python
def _validate_custom_rules(self, sql: str) -> List[ValidationIssue]:
    # Your custom validation logic
    pass
```

### Custom Edge Case Types

Extend `EdgeCaseGenerator` class:
```python
def generate_custom_edge_cases(self) -> List[GeneratedEdgeCase]:
    # Your edge case generation logic
    pass
```

## Performance Considerations

- **LLM Costs**: Tests using LLM planner incur API costs (~$0.001-0.005 per test)
- **Execution Time**: Full suite takes 2-5 minutes depending on configuration
- **BigQuery Usage**: Validation queries count against BigQuery quotas
- **Memory Usage**: Complex tests may require significant memory for large result processing

## Troubleshooting

### Common Issues

**Test initialization failures:**
- Check configuration file path and format
- Verify BigQuery credentials and permissions
- Ensure LookML files are accessible

**High failure rates:**
- Review LookML schema compatibility
- Check BigQuery dataset availability
- Verify field mappings in test cases

**Performance issues:**
- Use `--quick` mode for faster iteration
- Disable validation with `--no-validation` 
- Reduce test count with `--max-tests`

**Memory errors:**
- Reduce concurrent test execution
- Use `--no-performance` to disable tracking
- Filter to specific test categories

### Debug Mode

Enable verbose logging:
```bash
python run_comprehensive_tests.py --verbose
```

Check generated SQL:
```bash
# Results include generated SQL for each test
cat test_results/test_results_*.json | jq '.individual_results[0].sql_generated'
```

## Contributing

To contribute new test cases or improvements:

1. Add test cases following existing patterns
2. Include proper categorization and metadata
3. Test with both rule-based and LLM planners
4. Update documentation for new features
5. Verify performance impact

## License

This testing framework is part of the LookML Text-to-SQL project and follows the same license terms.