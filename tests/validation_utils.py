"""
SQL validation and performance benchmarking utilities.

This module provides advanced validation capabilities including:
- SQL syntax and semantic validation
- Query complexity analysis and optimization suggestions
- Performance profiling and bottleneck detection
- BigQuery-specific validation and best practices checking
- Cost estimation and resource usage analysis
"""

import re
import time
import sqlparse
from typing import Dict, List, Any, Optional, Tuple, NamedTuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class QueryComplexity(Enum):
    """Query complexity levels."""
    SIMPLE = "simple"
    MODERATE = "moderate" 
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"

class PerformanceIssue(Enum):
    """Types of performance issues."""
    MISSING_LIMIT = "missing_limit"
    COMPLEX_JOINS = "complex_joins"
    NESTED_SUBQUERIES = "nested_subqueries"
    CARTESIAN_PRODUCT = "cartesian_product"
    NO_WHERE_CLAUSE = "no_where_clause"
    EXPENSIVE_FUNCTIONS = "expensive_functions"
    LARGE_RESULT_SET = "large_result_set"
    INEFFICIENT_GROUPING = "inefficient_grouping"

@dataclass
class ValidationIssue:
    """Represents a validation issue found in SQL."""
    severity: str  # "error", "warning", "info"
    category: str  # "syntax", "performance", "best_practice", "cost"
    message: str
    line_number: Optional[int] = None
    suggestion: Optional[str] = None

@dataclass
class ComplexityMetrics:
    """Metrics for analyzing query complexity."""
    total_tables: int
    total_joins: int
    nested_subquery_depth: int
    aggregate_functions_count: int
    window_functions_count: int
    case_statements_count: int
    cte_count: int
    union_count: int
    complexity_score: float
    complexity_level: QueryComplexity

@dataclass
class PerformanceProfile:
    """Performance profile for a SQL query."""
    estimated_scan_bytes: Optional[int]
    estimated_cost_usd: Optional[float]
    potential_issues: List[PerformanceIssue]
    optimization_suggestions: List[str]
    complexity_metrics: ComplexityMetrics
    bigquery_specific_notes: List[str]

class SQLValidator:
    """Advanced SQL validation and analysis."""
    
    def __init__(self):
        """Initialize the validator."""
        # Common BigQuery functions that can be expensive
        self.expensive_functions = {
            'REGEXP_EXTRACT', 'REGEXP_REPLACE', 'REGEXP_CONTAINS',
            'JSON_EXTRACT', 'JSON_EXTRACT_SCALAR', 'PARSE_JSON',
            'STRING_AGG', 'ARRAY_AGG', 'APPROX_TOP_COUNT'
        }
        
        # Functions that require special attention for performance
        self.window_functions = {
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE',
            'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE',
            'SUM', 'COUNT', 'AVG', 'MIN', 'MAX'
        }
    
    def validate_sql(self, sql: str) -> Tuple[bool, List[ValidationIssue]]:
        """Comprehensive SQL validation."""
        issues = []
        
        try:
            # Parse SQL
            parsed = sqlparse.parse(sql)
            if not parsed:
                issues.append(ValidationIssue(
                    severity="error",
                    category="syntax", 
                    message="Failed to parse SQL statement"
                ))
                return False, issues
            
            statement = parsed[0]
            
            # Syntax validation
            syntax_issues = self._validate_syntax(sql, statement)
            issues.extend(syntax_issues)
            
            # Performance validation
            performance_issues = self._validate_performance(sql, statement)
            issues.extend(performance_issues)
            
            # Best practices validation
            best_practice_issues = self._validate_best_practices(sql, statement)
            issues.extend(best_practice_issues)
            
            # BigQuery specific validation
            bq_issues = self._validate_bigquery_specifics(sql, statement)
            issues.extend(bq_issues)
            
            # Determine if validation passed
            has_errors = any(issue.severity == "error" for issue in issues)
            return not has_errors, issues
            
        except Exception as e:
            logger.error(f"Validation failed with exception: {e}")
            issues.append(ValidationIssue(
                severity="error",
                category="syntax",
                message=f"Validation exception: {str(e)}"
            ))
            return False, issues
    
    def _validate_syntax(self, sql: str, statement) -> List[ValidationIssue]:
        """Validate SQL syntax."""
        issues = []
        
        # Check for common syntax errors
        sql_upper = sql.upper()
        
        # Unmatched parentheses
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            issues.append(ValidationIssue(
                severity="error",
                category="syntax",
                message=f"Unmatched parentheses: {open_parens} open, {close_parens} close"
            ))
        
        # Check for incomplete SQL statements
        if not sql_upper.strip().endswith(';') and 'SELECT' in sql_upper:
            # Check if it looks like a complete query
            required_keywords = ['SELECT', 'FROM']
            missing_keywords = [kw for kw in required_keywords if kw not in sql_upper]
            if missing_keywords:
                issues.append(ValidationIssue(
                    severity="error",
                    category="syntax",
                    message=f"Incomplete SQL statement, missing: {', '.join(missing_keywords)}"
                ))
        
        return issues
    
    def _validate_performance(self, sql: str, statement) -> List[ValidationIssue]:
        """Validate performance-related aspects."""
        issues = []
        sql_upper = sql.upper()
        
        # Check for LIMIT clause
        if 'SELECT' in sql_upper and 'LIMIT' not in sql_upper:
            issues.append(ValidationIssue(
                severity="warning",
                category="performance",
                message="Query lacks LIMIT clause - may return large result set",
                suggestion="Add LIMIT clause to control result set size"
            ))
        
        # Check for WHERE clause in SELECT statements
        if 'SELECT' in sql_upper and 'FROM' in sql_upper and 'WHERE' not in sql_upper:
            # Skip if it's an aggregation query
            if not any(func in sql_upper for func in ['GROUP BY', 'HAVING', 'COUNT(', 'SUM(', 'AVG(']):
                issues.append(ValidationIssue(
                    severity="warning", 
                    category="performance",
                    message="Query lacks WHERE clause - may scan entire table",
                    suggestion="Add WHERE clause to filter data and reduce scan size"
                ))
        
        # Check for Cartesian products (multiple FROM clauses without JOIN)
        from_count = sql_upper.count('FROM')
        join_count = sql_upper.count('JOIN')
        comma_joins = len(re.findall(r'FROM\s+\w+\s*,\s*\w+', sql_upper))
        
        if from_count > 1 and join_count == 0 and comma_joins == 0:
            issues.append(ValidationIssue(
                severity="error",
                category="performance", 
                message="Potential Cartesian product detected - multiple FROM clauses without JOINs",
                suggestion="Use explicit JOIN syntax to specify table relationships"
            ))
        
        # Check for expensive functions
        for func in self.expensive_functions:
            if func in sql_upper:
                issues.append(ValidationIssue(
                    severity="info",
                    category="performance",
                    message=f"Query uses potentially expensive function: {func}",
                    suggestion=f"Consider caching results or optimizing {func} usage"
                ))
        
        # Check for SELECT *
        if re.search(r'SELECT\s+\*', sql_upper):
            issues.append(ValidationIssue(
                severity="warning",
                category="performance",
                message="Query uses SELECT * - may select unnecessary columns",
                suggestion="Specify only required columns to reduce data transfer"
            ))
        
        return issues
    
    def _validate_best_practices(self, sql: str, statement) -> List[ValidationIssue]:
        """Validate SQL best practices."""
        issues = []
        sql_upper = sql.upper()
        
        # Check for table aliases
        if 'JOIN' in sql_upper:
            # Simple heuristic: if we have JOINs but no obvious aliases
            if not re.search(r'\w+\s+AS\s+\w+|\w+\s+\w+\s+ON', sql_upper):
                issues.append(ValidationIssue(
                    severity="info",
                    category="best_practice",
                    message="Consider using table aliases for complex queries with JOINs",
                    suggestion="Add aliases like 'FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id'"
                ))
        
        # Check for consistent naming conventions
        if re.search(r'[a-z]+[A-Z]', sql):  # Mixed case without underscores
            issues.append(ValidationIssue(
                severity="info",
                category="best_practice", 
                message="Consider using consistent snake_case naming convention",
                suggestion="Use snake_case for column and table names"
            ))
        
        return issues
    
    def _validate_bigquery_specifics(self, sql: str, statement) -> List[ValidationIssue]:
        """Validate BigQuery-specific aspects."""
        issues = []
        sql_upper = sql.upper()
        
        # Check for legacy SQL syntax
        if re.search(r'TABLE_DATE_RANGE|TABLE_QUERY', sql_upper):
            issues.append(ValidationIssue(
                severity="warning",
                category="best_practice",
                message="Query uses legacy SQL syntax",
                suggestion="Consider migrating to Standard SQL syntax"
            ))
        
        # Check for proper dataset qualification
        if not re.search(r'`[\w-]+\.[\w-]+\.[\w-]+`|[\w-]+\.[\w-]+\.[\w-]+', sql):
            if '.' in sql and 'SELECT' in sql_upper:
                issues.append(ValidationIssue(
                    severity="info", 
                    category="best_practice",
                    message="Consider using fully qualified table names (project.dataset.table)",
                    suggestion="Use format: `project.dataset.table` for clarity"
                ))
        
        return issues
    
    def analyze_complexity(self, sql: str) -> ComplexityMetrics:
        """Analyze query complexity and generate metrics."""
        sql_upper = sql.upper()
        
        # Count various complexity indicators
        total_tables = len(re.findall(r'FROM\s+[\w.`-]+|JOIN\s+[\w.`-]+', sql_upper))
        total_joins = sql_upper.count('JOIN')
        
        # Count nested subqueries
        nested_depth = self._calculate_nesting_depth(sql)
        
        # Count function types
        aggregate_functions = len(re.findall(
            r'\b(COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE)\s*\(', sql_upper
        ))
        
        window_functions = len(re.findall(
            r'\b(' + '|'.join(self.window_functions) + r')\s*\(\s*.*\)\s*OVER\s*\(', sql_upper
        ))
        
        case_statements = sql_upper.count('CASE')
        cte_count = sql_upper.count('WITH')
        union_count = sql_upper.count('UNION')
        
        # Calculate complexity score
        complexity_score = (
            total_tables * 1.0 +
            total_joins * 2.0 +
            nested_depth * 3.0 +
            aggregate_functions * 1.5 +
            window_functions * 2.5 +
            case_statements * 1.0 +
            cte_count * 2.0 +
            union_count * 1.5
        )
        
        # Determine complexity level
        if complexity_score <= 5:
            complexity_level = QueryComplexity.SIMPLE
        elif complexity_score <= 15:
            complexity_level = QueryComplexity.MODERATE
        elif complexity_score <= 30:
            complexity_level = QueryComplexity.COMPLEX
        else:
            complexity_level = QueryComplexity.VERY_COMPLEX
        
        return ComplexityMetrics(
            total_tables=total_tables,
            total_joins=total_joins,
            nested_subquery_depth=nested_depth,
            aggregate_functions_count=aggregate_functions,
            window_functions_count=window_functions,
            case_statements_count=case_statements,
            cte_count=cte_count,
            union_count=union_count,
            complexity_score=complexity_score,
            complexity_level=complexity_level
        )
    
    def _calculate_nesting_depth(self, sql: str) -> int:
        """Calculate maximum nesting depth of subqueries."""
        depth = 0
        max_depth = 0
        
        # Simple approach: count nested parentheses with SELECT
        sql_upper = sql.upper()
        i = 0
        while i < len(sql_upper):
            if sql_upper[i] == '(':
                # Look ahead to see if this starts a subquery
                remaining = sql_upper[i:i+50]  # Look at next 50 chars
                if 'SELECT' in remaining:
                    depth += 1
                    max_depth = max(max_depth, depth)
            elif sql_upper[i] == ')':
                if depth > 0:
                    depth -= 1
            i += 1
        
        return max_depth
    
    def generate_performance_profile(self, sql: str, 
                                   estimated_bytes: Optional[int] = None) -> PerformanceProfile:
        """Generate comprehensive performance profile for SQL query."""
        
        # Analyze complexity
        complexity_metrics = self.analyze_complexity(sql)
        
        # Identify potential issues
        potential_issues = []
        optimization_suggestions = []
        bigquery_notes = []
        
        sql_upper = sql.upper()
        
        # Check for performance issues
        if 'LIMIT' not in sql_upper:
            potential_issues.append(PerformanceIssue.MISSING_LIMIT)
            optimization_suggestions.append("Add LIMIT clause to control result set size")
        
        if complexity_metrics.total_joins > 3:
            potential_issues.append(PerformanceIssue.COMPLEX_JOINS)
            optimization_suggestions.append("Consider breaking down complex joins or using CTEs")
        
        if complexity_metrics.nested_subquery_depth > 2:
            potential_issues.append(PerformanceIssue.NESTED_SUBQUERIES)
            optimization_suggestions.append("Consider flattening nested queries using CTEs or JOINs")
        
        if 'WHERE' not in sql_upper and 'SELECT' in sql_upper and 'FROM' in sql_upper:
            potential_issues.append(PerformanceIssue.NO_WHERE_CLAUSE)
            optimization_suggestions.append("Add WHERE clause to filter data early")
        
        # BigQuery specific optimizations
        if complexity_metrics.window_functions_count > 0:
            bigquery_notes.append("Window functions can be expensive - consider partitioning")
        
        if complexity_metrics.total_joins > 2:
            bigquery_notes.append("Complex joins may benefit from table clustering")
        
        # Estimate cost (rough approximation)
        estimated_cost = None
        if estimated_bytes:
            # BigQuery charges ~$5 per TB processed
            estimated_cost = (estimated_bytes / 1e12) * 5.0
        
        return PerformanceProfile(
            estimated_scan_bytes=estimated_bytes,
            estimated_cost_usd=estimated_cost,
            potential_issues=potential_issues,
            optimization_suggestions=optimization_suggestions,
            complexity_metrics=complexity_metrics,
            bigquery_specific_notes=bigquery_notes
        )

class PerformanceBenchmarker:
    """Benchmarks SQL query performance."""
    
    def __init__(self, bigquery_client=None):
        """Initialize benchmarker."""
        self.bigquery_client = bigquery_client
    
    def benchmark_query_plan(self, sql: str) -> Dict[str, Any]:
        """Analyze query execution plan without running the query."""
        if not self.bigquery_client:
            return {"error": "BigQuery client not available"}
        
        try:
            # Use dry_run to get query plan
            job_config = self.bigquery_client.QueryJobConfig(dry_run=True)
            query_job = self.bigquery_client.query(sql, job_config=job_config)
            
            return {
                "total_bytes_processed": query_job.total_bytes_processed,
                "total_bytes_billed": query_job.total_bytes_billed,
                "estimated_cost_usd": (query_job.total_bytes_billed / 1e12) * 5.0,  # $5 per TB
                "cache_hit": query_job.cache_hit if hasattr(query_job, 'cache_hit') else None,
                "query_plan_available": True
            }
        
        except Exception as e:
            return {
                "error": str(e),
                "query_plan_available": False
            }
    
    def measure_execution_time(self, sql: str, iterations: int = 1) -> Dict[str, Any]:
        """Measure actual query execution time (use with caution on large datasets)."""
        if not self.bigquery_client:
            return {"error": "BigQuery client not available"}
        
        if iterations > 1:
            logger.warning(f"Running query {iterations} times - this may incur significant costs")
        
        execution_times = []
        total_bytes_processed = None
        
        try:
            for i in range(iterations):
                start_time = time.time()
                
                query_job = self.bigquery_client.query(sql)
                results = query_job.result()  # Wait for completion
                
                execution_time = time.time() - start_time
                execution_times.append(execution_time)
                
                if total_bytes_processed is None:
                    total_bytes_processed = query_job.total_bytes_processed
            
            return {
                "execution_times": execution_times,
                "avg_execution_time": sum(execution_times) / len(execution_times),
                "min_execution_time": min(execution_times),
                "max_execution_time": max(execution_times),
                "total_bytes_processed": total_bytes_processed,
                "iterations": iterations
            }
        
        except Exception as e:
            return {
                "error": str(e),
                "completed_iterations": len(execution_times)
            }

def validate_test_sql(sql: str) -> Tuple[bool, Dict[str, Any]]:
    """Comprehensive validation function for test cases."""
    validator = SQLValidator()
    
    # Basic validation
    is_valid, issues = validator.validate_sql(sql)
    
    # Complexity analysis
    complexity_metrics = validator.analyze_complexity(sql)
    
    # Performance profile
    performance_profile = validator.generate_performance_profile(sql)
    
    return is_valid, {
        "validation_passed": is_valid,
        "issues": [
            {
                "severity": issue.severity,
                "category": issue.category, 
                "message": issue.message,
                "line_number": issue.line_number,
                "suggestion": issue.suggestion
            } for issue in issues
        ],
        "complexity_metrics": {
            "total_tables": complexity_metrics.total_tables,
            "total_joins": complexity_metrics.total_joins,
            "nested_depth": complexity_metrics.nested_subquery_depth,
            "complexity_score": complexity_metrics.complexity_score,
            "complexity_level": complexity_metrics.complexity_level.value
        },
        "performance_profile": {
            "potential_issues": [issue.value for issue in performance_profile.potential_issues],
            "optimization_suggestions": performance_profile.optimization_suggestions,
            "bigquery_notes": performance_profile.bigquery_specific_notes
        },
        "error_count": len([i for i in issues if i.severity == "error"]),
        "warning_count": len([i for i in issues if i.severity == "warning"]),
        "info_count": len([i for i in issues if i.severity == "info"])
    }