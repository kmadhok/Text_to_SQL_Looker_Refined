"""
Automated test runner for complex LookML Text-to-SQL scenarios.

This module provides comprehensive testing capabilities including:
- Automated execution of test cases
- Performance benchmarking and profiling  
- SQL validation and syntax checking
- LLM cost tracking and token usage analysis
- Detailed reporting and result analysis
"""

import time
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import traceback

from test_complex_scenarios import (
    ALL_TEST_CASES, TestCase, TestCategory, DifficultyLevel,
    get_test_cases_by_category, get_test_cases_by_difficulty,
    get_expected_success_cases, get_expected_failure_cases
)

# Import the main engine (adjust path as needed)
import sys
sys.path.append('../')
from src.main import TextToSQLEngine

@dataclass
class TestResult:
    """Represents the result of executing a single test case."""
    test_case: TestCase
    success: bool
    sql_generated: Optional[str]
    error_message: Optional[str]
    execution_time: float
    validation_passed: Optional[bool]
    validation_error: Optional[str]
    llm_cost_estimate: float
    llm_token_usage: Dict[str, Any]
    explore_used: Optional[str]
    fields_selected: List[str]
    joins_required: List[str]
    limit_applied: bool
    expected_outcome_met: bool
    performance_notes: Optional[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Convert test_case to dict and handle enums
        result['test_case'] = asdict(self.test_case)
        result['test_case']['category'] = self.test_case.category.value
        result['test_case']['difficulty'] = self.test_case.difficulty.value
        return result

@dataclass 
class TestSuiteResults:
    """Aggregated results for the entire test suite."""
    timestamp: str
    total_tests: int
    passed_tests: int
    failed_tests: int
    expected_failures: int
    unexpected_failures: int
    total_execution_time: float
    total_llm_cost: float
    results_by_category: Dict[str, Dict[str, int]]
    results_by_difficulty: Dict[str, Dict[str, int]]
    individual_results: List[TestResult]
    
    @property
    def success_rate(self) -> float:
        """Calculate overall success rate."""
        if self.total_tests == 0:
            return 0.0
        return (self.passed_tests / self.total_tests) * 100
    
    @property
    def expected_success_rate(self) -> float:
        """Calculate success rate for tests expected to succeed."""
        expected_success_cases = len(get_expected_success_cases())
        if expected_success_cases == 0:
            return 0.0
        actual_successes = sum(1 for r in self.individual_results 
                             if r.success and r.test_case.should_succeed)
        return (actual_successes / expected_success_cases) * 100

class ComplexScenarioTestRunner:
    """Main test runner for complex LookML Text-to-SQL scenarios."""
    
    def __init__(self, config_path: Optional[str] = None, 
                 output_dir: str = "test_results",
                 enable_validation: bool = True,
                 enable_performance_tracking: bool = True):
        """Initialize the test runner."""
        self.config_path = config_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.enable_validation = enable_validation
        self.enable_performance_tracking = enable_performance_tracking
        
        # Setup logging
        log_file = self.output_dir / f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize engine
        self.logger.info("Initializing TextToSQLEngine for testing")
        self.engine = None
    
    def _initialize_engine(self) -> bool:
        """Initialize the engine with error handling."""
        try:
            self.engine = TextToSQLEngine(self.config_path)
            if self.enable_validation:
                self.engine.config.generator.enable_dry_run = True
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize engine: {e}")
            return False
    
    def _execute_single_test(self, test_case: TestCase) -> TestResult:
        """Execute a single test case and return results."""
        self.logger.info(f"Executing test {test_case.id}: {test_case.query[:50]}...")
        
        start_time = time.time()
        
        try:
            # Generate SQL using the engine
            result = self.engine.generate_sql(test_case.query)
            execution_time = time.time() - start_time
            
            # Determine if test met expected outcome
            expected_outcome_met = self._evaluate_expected_outcome(test_case, result)
            
            # Create test result
            test_result = TestResult(
                test_case=test_case,
                success=result.get('error') is None,
                sql_generated=result.get('sql'),
                error_message=result.get('error'),
                execution_time=execution_time,
                validation_passed=result.get('validation_passed'),
                validation_error=result.get('error') if result.get('validation_passed') is False else None,
                llm_cost_estimate=result.get('llm_cost_estimate', 0.0),
                llm_token_usage=result.get('llm_token_usage', {}),
                explore_used=result.get('explore_used'),
                fields_selected=result.get('fields_selected', []),
                joins_required=result.get('joins_required', []),
                limit_applied=result.get('limit_applied', False),
                expected_outcome_met=expected_outcome_met,
                performance_notes=self._generate_performance_notes(test_case, result, execution_time)
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Test {test_case.id} failed with exception: {e}")
            
            test_result = TestResult(
                test_case=test_case,
                success=False,
                sql_generated=None,
                error_message=f"Exception: {str(e)}",
                execution_time=execution_time,
                validation_passed=False,
                validation_error=str(e),
                llm_cost_estimate=0.0,
                llm_token_usage={},
                explore_used=None,
                fields_selected=[],
                joins_required=[],
                limit_applied=False,
                expected_outcome_met=not test_case.should_succeed,  # Exception might be expected
                performance_notes=f"Failed with exception after {execution_time:.2f}s"
            )
        
        return test_result
    
    def _evaluate_expected_outcome(self, test_case: TestCase, result: Dict[str, Any]) -> bool:
        """Evaluate if the test result matches expected outcome."""
        has_error = result.get('error') is not None
        
        if test_case.should_succeed:
            # Test should succeed - check if it actually did
            if has_error:
                return False
            
            # Additional checks for successful cases
            if not result.get('sql'):
                return False
            
            # Check if required explores/fields were used (if specified)
            if test_case.required_explores:
                explore_used = result.get('explore_used', '')
                if not any(explore in explore_used for explore in test_case.required_explores):
                    self.logger.warning(f"Test {test_case.id}: Expected explore not used")
            
            return True
        else:
            # Test should fail - check if it failed appropriately
            return has_error
    
    def _generate_performance_notes(self, test_case: TestCase, result: Dict[str, Any], 
                                  execution_time: float) -> Optional[str]:
        """Generate performance analysis notes."""
        if not self.enable_performance_tracking:
            return None
        
        notes = []
        
        # Execution time analysis
        if execution_time > 10.0:
            notes.append(f"Slow execution: {execution_time:.2f}s")
        elif execution_time > 5.0:
            notes.append(f"Moderate execution time: {execution_time:.2f}s") 
        
        # LLM cost analysis
        llm_cost = result.get('llm_cost_estimate', 0.0)
        if llm_cost > 0.01:
            notes.append(f"High LLM cost: ${llm_cost:.4f}")
        
        # Token usage analysis
        token_usage = result.get('llm_token_usage', {})
        if token_usage.get('total_tokens', 0) > 10000:
            notes.append(f"High token usage: {token_usage.get('total_tokens')} tokens")
        
        # Join complexity analysis
        joins_count = len(result.get('joins_required', []))
        if joins_count > 3:
            notes.append(f"Complex joins: {joins_count} joins required")
        
        return "; ".join(notes) if notes else None
    
    def run_test_suite(self, 
                      categories: Optional[List[TestCategory]] = None,
                      difficulties: Optional[List[DifficultyLevel]] = None,
                      test_ids: Optional[List[str]] = None,
                      max_tests: Optional[int] = None) -> TestSuiteResults:
        """Run the full test suite or a subset of tests."""
        
        # Initialize engine
        if not self._initialize_engine():
            raise RuntimeError("Failed to initialize TextToSQLEngine")
        
        # Filter test cases based on parameters
        test_cases = self._filter_test_cases(categories, difficulties, test_ids, max_tests)
        
        self.logger.info(f"Running {len(test_cases)} test cases")
        
        # Execute all tests
        start_time = time.time()
        individual_results = []
        
        for i, test_case in enumerate(test_cases, 1):
            self.logger.info(f"Running test {i}/{len(test_cases)}: {test_case.id}")
            result = self._execute_single_test(test_case)
            individual_results.append(result)
            
            # Log progress
            if i % 5 == 0:
                self.logger.info(f"Completed {i}/{len(test_cases)} tests")
        
        total_execution_time = time.time() - start_time
        
        # Aggregate results
        suite_results = self._aggregate_results(individual_results, total_execution_time)
        
        # Save results
        self._save_results(suite_results)
        
        # Print summary
        self._print_summary(suite_results)
        
        return suite_results
    
    def _filter_test_cases(self, categories: Optional[List[TestCategory]], 
                          difficulties: Optional[List[DifficultyLevel]],
                          test_ids: Optional[List[str]],
                          max_tests: Optional[int]) -> List[TestCase]:
        """Filter test cases based on criteria."""
        
        test_cases = ALL_TEST_CASES.copy()
        
        # Filter by categories
        if categories:
            test_cases = [tc for tc in test_cases if tc.category in categories]
        
        # Filter by difficulties  
        if difficulties:
            test_cases = [tc for tc in test_cases if tc.difficulty in difficulties]
        
        # Filter by test IDs
        if test_ids:
            test_cases = [tc for tc in test_cases if tc.id in test_ids]
        
        # Limit number of tests
        if max_tests and len(test_cases) > max_tests:
            test_cases = test_cases[:max_tests]
        
        return test_cases
    
    def _aggregate_results(self, results: List[TestResult], 
                          total_execution_time: float) -> TestSuiteResults:
        """Aggregate individual test results into suite results."""
        
        total_tests = len(results)
        passed_tests = sum(1 for r in results if r.success)
        failed_tests = total_tests - passed_tests
        
        expected_failures = sum(1 for r in results 
                               if not r.success and not r.test_case.should_succeed)
        unexpected_failures = failed_tests - expected_failures
        
        total_llm_cost = sum(r.llm_cost_estimate for r in results)
        
        # Results by category
        results_by_category = {}
        for category in TestCategory:
            category_results = [r for r in results if r.test_case.category == category]
            if category_results:
                results_by_category[category.value] = {
                    'total': len(category_results),
                    'passed': sum(1 for r in category_results if r.success),
                    'failed': sum(1 for r in category_results if not r.success)
                }
        
        # Results by difficulty
        results_by_difficulty = {}
        for difficulty in DifficultyLevel:
            difficulty_results = [r for r in results if r.test_case.difficulty == difficulty]
            if difficulty_results:
                results_by_difficulty[difficulty.value] = {
                    'total': len(difficulty_results),
                    'passed': sum(1 for r in difficulty_results if r.success),
                    'failed': sum(1 for r in difficulty_results if not r.success)
                }
        
        return TestSuiteResults(
            timestamp=datetime.now().isoformat(),
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            expected_failures=expected_failures,
            unexpected_failures=unexpected_failures,
            total_execution_time=total_execution_time,
            total_llm_cost=total_llm_cost,
            results_by_category=results_by_category,
            results_by_difficulty=results_by_difficulty,
            individual_results=results
        )
    
    def _save_results(self, results: TestSuiteResults) -> None:
        """Save test results to files."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save detailed JSON results
        json_file = self.output_dir / f"test_results_{timestamp}.json"
        with open(json_file, 'w') as f:
            # Convert to dict for JSON serialization
            results_dict = asdict(results)
            results_dict['individual_results'] = [r.to_dict() for r in results.individual_results]
            json.dump(results_dict, f, indent=2, default=str)
        
        self.logger.info(f"Detailed results saved to {json_file}")
        
        # Save summary report
        report_file = self.output_dir / f"test_summary_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(self._generate_text_report(results))
        
        self.logger.info(f"Summary report saved to {report_file}")
    
    def _print_summary(self, results: TestSuiteResults) -> None:
        """Print test results summary to console."""
        print("\n" + "="*60)
        print("TEST SUITE EXECUTION SUMMARY")
        print("="*60)
        print(f"Total Tests: {results.total_tests}")
        print(f"Passed: {results.passed_tests}")
        print(f"Failed: {results.failed_tests}")
        print(f"Success Rate: {results.success_rate:.1f}%")
        print(f"Expected Success Rate: {results.expected_success_rate:.1f}%")
        print(f"Total Execution Time: {results.total_execution_time:.2f}s")
        print(f"Total LLM Cost: ${results.total_llm_cost:.4f}")
        print(f"Unexpected Failures: {results.unexpected_failures}")
        
        if results.unexpected_failures > 0:
            print("\nUNEXPECTED FAILURES:")
            for result in results.individual_results:
                if not result.success and result.test_case.should_succeed:
                    print(f"  {result.test_case.id}: {result.error_message}")
    
    def _generate_text_report(self, results: TestSuiteResults) -> str:
        """Generate detailed text report."""
        lines = []
        lines.append("LookML Text-to-SQL Complex Scenario Test Report")
        lines.append("=" * 50)
        lines.append(f"Generated: {results.timestamp}")
        lines.append("")
        
        # Summary statistics
        lines.append("SUMMARY STATISTICS")
        lines.append("-" * 20)
        lines.append(f"Total Tests: {results.total_tests}")
        lines.append(f"Passed: {results.passed_tests}")
        lines.append(f"Failed: {results.failed_tests}")
        lines.append(f"Success Rate: {results.success_rate:.1f}%")
        lines.append(f"Expected Success Rate: {results.expected_success_rate:.1f}%")
        lines.append(f"Total Execution Time: {results.total_execution_time:.2f}s")
        lines.append(f"Average Test Time: {results.total_execution_time/results.total_tests:.2f}s")
        lines.append(f"Total LLM Cost: ${results.total_llm_cost:.4f}")
        lines.append("")
        
        # Results by category
        lines.append("RESULTS BY CATEGORY")
        lines.append("-" * 20)
        for category, stats in results.results_by_category.items():
            success_rate = (stats['passed'] / stats['total']) * 100
            lines.append(f"{category}: {stats['passed']}/{stats['total']} ({success_rate:.1f}%)")
        lines.append("")
        
        # Results by difficulty
        lines.append("RESULTS BY DIFFICULTY")
        lines.append("-" * 20)
        for difficulty, stats in results.results_by_difficulty.items():
            success_rate = (stats['passed'] / stats['total']) * 100
            lines.append(f"{difficulty}: {stats['passed']}/{stats['total']} ({success_rate:.1f}%)")
        lines.append("")
        
        # Individual test details
        lines.append("INDIVIDUAL TEST RESULTS")
        lines.append("-" * 30)
        for result in results.individual_results:
            status = "PASS" if result.success else "FAIL"
            expected = "✓" if result.expected_outcome_met else "✗"
            lines.append(f"{result.test_case.id} [{status}] {expected} - {result.test_case.query[:60]}...")
            if result.error_message:
                lines.append(f"  Error: {result.error_message}")
            if result.performance_notes:
                lines.append(f"  Performance: {result.performance_notes}")
            lines.append("")
        
        return "\n".join(lines)

def main():
    """Main entry point for running tests."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run complex LookML Text-to-SQL test scenarios")
    parser.add_argument("--config", help="Path to config file")
    parser.add_argument("--output-dir", default="test_results", help="Output directory for results")
    parser.add_argument("--category", choices=[c.value for c in TestCategory], 
                       action="append", help="Filter by category")
    parser.add_argument("--difficulty", choices=[d.value for d in DifficultyLevel],
                       action="append", help="Filter by difficulty")
    parser.add_argument("--test-id", action="append", help="Run specific test IDs")
    parser.add_argument("--max-tests", type=int, help="Maximum number of tests to run")
    parser.add_argument("--no-validation", action="store_true", help="Disable SQL validation")
    parser.add_argument("--no-performance", action="store_true", help="Disable performance tracking")
    
    args = parser.parse_args()
    
    # Convert string arguments back to enums
    categories = [TestCategory(c) for c in args.category] if args.category else None
    difficulties = [DifficultyLevel(d) for d in args.difficulty] if args.difficulty else None
    
    # Create and run test runner
    runner = ComplexScenarioTestRunner(
        config_path=args.config,
        output_dir=args.output_dir,
        enable_validation=not args.no_validation,
        enable_performance_tracking=not args.no_performance
    )
    
    try:
        results = runner.run_test_suite(
            categories=categories,
            difficulties=difficulties, 
            test_ids=args.test_id,
            max_tests=args.max_tests
        )
        
        # Exit with error code if there were unexpected failures
        exit_code = 0 if results.unexpected_failures == 0 else 1
        exit(exit_code)
        
    except Exception as e:
        print(f"Test runner failed: {e}")
        traceback.print_exc()
        exit(2)

if __name__ == "__main__":
    main()