#!/usr/bin/env python3
"""
Simple analyzer for test results.

This script helps you analyze the JSON output from simple_test_runner.py
to understand what's working, what's not, and why.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any

def load_results(file_path: str) -> Dict[str, Any]:
    """Load test results from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def analyze_success_patterns(data: Dict[str, Any]) -> None:
    """Analyze patterns in successful vs failed queries."""
    
    results = data['results']
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print("SUCCESS/FAILURE ANALYSIS")
    print("=" * 30)
    print(f"Successful: {len(successful)}/{len(results)} ({len(successful)/len(results)*100:.1f}%)")
    print(f"Failed: {len(failed)}/{len(results)} ({len(failed)/len(results)*100:.1f}%)")
    
    if failed:
        print(f"\nFAILED QUERIES:")
        for result in failed:
            print(f"  {result['question']}")
            print(f"    → {result['error_message']}")
            print()

def analyze_schema_usage(data: Dict[str, Any]) -> None:
    """Analyze what schema elements are being found and used."""
    
    results = data['results']
    successful = [r for r in results if r['success']]
    
    print("SCHEMA USAGE ANALYSIS")
    print("=" * 25)
    
    if not successful:
        print("No successful queries to analyze")
        return
    
    # Analyze explores used
    explores_used = {}
    for result in successful:
        explore = result.get('explore_used')
        if explore:
            explores_used[explore] = explores_used.get(explore, 0) + 1
    
    print("Explores used:")
    for explore, count in sorted(explores_used.items(), key=lambda x: x[1], reverse=True):
        print(f"  {explore}: {count} times")
    
    # Analyze field patterns
    all_fields = []
    for result in successful:
        fields = result.get('fields_selected', [])
        all_fields.extend(fields)
    
    if all_fields:
        field_counts = {}
        for field in all_fields:
            field_counts[field] = field_counts.get(field, 0) + 1
        
        print(f"\nMost used fields:")
        sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
        for field, count in sorted_fields[:10]:  # Top 10
            print(f"  {field}: {count} times")
    
    # Analyze joins
    all_joins = []
    for result in successful:
        joins = result.get('joins_required', [])
        all_joins.extend(joins)
    
    if all_joins:
        join_counts = {}
        for join in all_joins:
            join_counts[join] = join_counts.get(join, 0) + 1
        
        print(f"\nJoins required:")
        for join, count in sorted(join_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {join}: {count} times")

def analyze_llm_usage(data: Dict[str, Any]) -> None:
    """Analyze LLM usage patterns and costs."""
    
    results = data['results']
    llm_results = [r for r in results if r.get('llm_used', False)]
    
    print("LLM USAGE ANALYSIS")
    print("=" * 20)
    
    if not llm_results:
        print("No LLM usage detected (using rule-based planner)")
        return
    
    total_cost = sum(r.get('llm_cost', 0) for r in llm_results)
    avg_cost = total_cost / len(llm_results) if llm_results else 0
    
    print(f"Queries using LLM: {len(llm_results)}/{len(results)}")
    print(f"Total LLM cost: ${total_cost:.4f}")
    print(f"Average cost per LLM query: ${avg_cost:.6f}")
    
    # Check how many have captured conversation details
    conversation_captured = [r for r in llm_results if r.get('llm_prompt_sent')]
    print(f"Conversation details captured: {len(conversation_captured)}/{len(llm_results)}")
    
    # Show most expensive queries
    expensive_queries = sorted(llm_results, key=lambda x: x.get('llm_cost', 0), reverse=True)
    print(f"\nMost expensive queries:")
    for result in expensive_queries[:5]:
        cost = result.get('llm_cost', 0)
        print(f"  ${cost:.4f} - {result['question']}")
    
    # Token usage analysis
    total_tokens = {}
    token_counts = 0
    for result in llm_results:
        tokens = result.get('llm_tokens', {})
        if tokens:
            token_counts += 1
            for key, value in tokens.items():
                if isinstance(value, (int, float)):
                    total_tokens[key] = total_tokens.get(key, 0) + value
    
    if total_tokens:
        print(f"\nToken usage (across {token_counts} queries):")
        for token_type, total in total_tokens.items():
            avg_tokens = total / token_counts
            print(f"  {token_type}: {total} total ({avg_tokens:.1f} avg)")
    
    # Analyze LLM conversation patterns if available
    if conversation_captured:
        print(f"\nLLM CONVERSATION ANALYSIS:")
        print("-" * 30)
        
        # Analyze prompt sizes
        prompt_sizes = [len(r.get('llm_prompt_sent', '')) for r in conversation_captured if r.get('llm_prompt_sent')]
        if prompt_sizes:
            avg_prompt_size = sum(prompt_sizes) / len(prompt_sizes)
            print(f"Average prompt size: {avg_prompt_size:.0f} characters")
            print(f"Max prompt size: {max(prompt_sizes)} characters")
        
        # Analyze schema context sizes
        schema_sizes = [len(r.get('llm_schema_context', '')) for r in conversation_captured if r.get('llm_schema_context')]
        if schema_sizes:
            avg_schema_size = sum(schema_sizes) / len(schema_sizes)
            print(f"Average schema context size: {avg_schema_size:.0f} characters")
        
        # Show some example prompts/responses
        print(f"\nSample LLM conversations (showing first 3):")
        for i, result in enumerate(conversation_captured[:3], 1):
            print(f"\nExample {i}: {result['question']}")
            
            prompt = result.get('llm_prompt_sent', '')
            if prompt:
                # Extract the natural language query part
                if 'Natural Language Query:' in prompt:
                    query_part = prompt.split('Natural Language Query:')[1].split('\n')[0].strip()
                    print(f"  Query sent to LLM: {query_part}")
                
                # Show schema context snippet
                schema = result.get('llm_schema_context', '')
                if schema and len(schema) > 100:
                    print(f"  Schema context: {len(schema)} chars (e.g., \"{schema[:100]}...\")")
            
            # Show LLM response snippet
            response = result.get('llm_raw_response', '')
            if response:
                # Clean up the response for display
                clean_response = response.replace('\n', ' ').replace('  ', ' ')
                if len(clean_response) > 150:
                    clean_response = clean_response[:150] + "..."
                print(f"  LLM response: {clean_response}")
        
        # Show conversation IDs if available
        conversation_ids = [r.get('llm_conversation_id') for r in conversation_captured if r.get('llm_conversation_id')]
        if conversation_ids:
            print(f"\nConversation IDs captured: {len(conversation_ids)}")
            print(f"Example conversation IDs: {', '.join(conversation_ids[:3])}")

def analyze_performance(data: Dict[str, Any]) -> None:
    """Analyze query performance patterns."""
    
    results = data['results']
    
    print("PERFORMANCE ANALYSIS")
    print("=" * 21)
    
    execution_times = [r['execution_time'] for r in results]
    avg_time = sum(execution_times) / len(execution_times)
    
    print(f"Total execution time: {data['total_execution_time']:.2f}s")
    print(f"Average query time: {avg_time:.2f}s")
    
    # Find slowest queries
    slow_queries = sorted(results, key=lambda x: x['execution_time'], reverse=True)
    print(f"\nSlowest queries:")
    for result in slow_queries[:5]:
        time_taken = result['execution_time']
        status = "✓" if result['success'] else "✗"
        print(f"  {time_taken:.2f}s {status} - {result['question']}")
    
    # Performance by success/failure
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    if successful:
        avg_success_time = sum(r['execution_time'] for r in successful) / len(successful)
        print(f"\nAverage time for successful queries: {avg_success_time:.2f}s")
    
    if failed:
        avg_fail_time = sum(r['execution_time'] for r in failed) / len(failed)
        print(f"Average time for failed queries: {avg_fail_time:.2f}s")

def show_sample_outputs(data: Dict[str, Any], num_samples: int = 3) -> None:
    """Show sample SQL outputs from successful queries."""
    
    results = data['results']
    successful = [r for r in results if r['success'] and r.get('sql_output')]
    
    print("SAMPLE SQL OUTPUTS")
    print("=" * 20)
    
    if not successful:
        print("No successful queries with SQL output found")
        return
    
    # Show a few examples
    samples = successful[:num_samples]
    
    for i, result in enumerate(samples, 1):
        print(f"\nSample {i}:")
        print(f"Question: {result['question']}")
        print(f"Explore: {result.get('explore_used', 'N/A')}")
        print(f"SQL:")
        sql = result['sql_output']
        # Pretty print SQL (simple formatting)
        if sql:
            lines = sql.split('\n')
            for line in lines:
                print(f"  {line}")
        else:
            print("  (No SQL generated)")

def show_llm_conversations(data: Dict[str, Any], num_samples: int = 2) -> None:
    """Show detailed LLM conversation examples."""
    
    results = data['results']
    llm_results = [r for r in results if r.get('llm_prompt_sent') and r.get('llm_raw_response')]
    
    print("DETAILED LLM CONVERSATIONS")
    print("=" * 30)
    
    if not llm_results:
        print("No LLM conversations captured")
        return
    
    samples = llm_results[:num_samples]
    
    for i, result in enumerate(samples, 1):
        print(f"\nConversation {i}: {result['question']}")
        print("-" * 50)
        
        # Show system prompt (first 200 chars)
        system_prompt = result.get('llm_system_prompt', '')
        if system_prompt:
            print(f"System Prompt: {system_prompt[:200]}...")
            print()
        
        # Show schema context (first 300 chars)
        schema_context = result.get('llm_schema_context', '')
        if schema_context:
            print(f"Schema Context ({len(schema_context)} chars):")
            print(f"  {schema_context[:300]}...")
            print()
        
        # Show user prompt
        user_prompt = result.get('llm_prompt_sent', '')
        if user_prompt:
            print(f"User Prompt:")
            print(f"  {user_prompt}")
            print()
        
        # Show LLM response
        llm_response = result.get('llm_raw_response', '')
        if llm_response:
            print(f"LLM Response:")
            print(f"  {llm_response}")
            print()
        
        # Show final SQL
        final_sql = result.get('sql_output', '')
        if final_sql:
            print(f"Final SQL Generated:")
            lines = final_sql.split('\n')
            for line in lines:
                print(f"  {line}")
        
        print("-" * 50)

def generate_insights(data: Dict[str, Any]) -> None:
    """Generate insights and recommendations."""
    
    results = data['results']
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print("INSIGHTS & RECOMMENDATIONS")
    print("=" * 30)
    
    success_rate = len(successful) / len(results) * 100
    
    if success_rate >= 80:
        print("✓ Good success rate! System is working well overall.")
    elif success_rate >= 60:
        print("⚠ Moderate success rate. Some improvements needed.")
    else:
        print("✗ Low success rate. Significant issues need addressing.")
    
    # Common failure patterns
    if failed:
        error_patterns = {}
        for result in failed:
            error = result.get('error_message', 'Unknown error')
            # Categorize errors
            if 'Exception' in error:
                category = 'System Exception'
            elif 'Could not generate' in error:
                category = 'Planning Failure'
            elif 'validation' in error.lower():
                category = 'Validation Failure'
            else:
                category = 'Other Error'
            
            error_patterns[category] = error_patterns.get(category, 0) + 1
        
        print(f"\nError patterns:")
        for pattern, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"  {pattern}: {count} occurrences")
    
    # Schema coverage
    explores_used = set()
    for result in successful:
        explore = result.get('explore_used')
        if explore:
            explores_used.add(explore)
    
    if explores_used:
        print(f"\nSchema coverage: {len(explores_used)} explores used")
        print(f"Explores: {', '.join(sorted(explores_used))}")
    
    # Recommendations
    print(f"\nRecommendations:")
    
    if len(failed) > 0:
        print(f"- Investigate the {len(failed)} failed queries to improve coverage")
    
    if data.get('total_llm_cost', 0) > 0.10:
        print(f"- Consider optimizing LLM usage (current cost: ${data['total_llm_cost']:.4f})")
    
    avg_time = data.get('total_execution_time', 0) / len(results)
    if avg_time > 5.0:
        print(f"- Optimize performance (avg {avg_time:.2f}s per query)")
    
    validation_failures = sum(1 for r in results if r.get('validation_passed') is False)
    if validation_failures > 0:
        print(f"- Review {validation_failures} SQL validation failures")

def main():
    """Main analyzer entry point."""
    
    parser = argparse.ArgumentParser(description="Analyze simple test results")
    parser.add_argument("results_file", help="Path to JSON results file")
    parser.add_argument("--samples", type=int, default=3, help="Number of SQL samples to show")
    parser.add_argument("--llm-conversations", type=int, default=2, help="Number of LLM conversations to show in detail")
    
    args = parser.parse_args()
    
    if not Path(args.results_file).exists():
        print(f"Results file not found: {args.results_file}")
        return 1
    
    try:
        print(f"Loading results from {args.results_file}")
        data = load_results(args.results_file)
        
        print(f"Test run from: {data.get('timestamp', 'Unknown')}")
        print(f"Questions tested: {data.get('total_questions', 'Unknown')}")
        print()
        
        # Run analyses
        analyze_success_patterns(data)
        print()
        
        analyze_schema_usage(data)
        print()
        
        analyze_llm_usage(data)
        print()
        
        analyze_performance(data)
        print()
        
        show_sample_outputs(data, args.samples)
        print()
        
        show_llm_conversations(data, args.llm_conversations)
        print()
        
        generate_insights(data)
        
    except Exception as e:
        print(f"Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())