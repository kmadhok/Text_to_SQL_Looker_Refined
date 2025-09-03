#!/usr/bin/env python3
"""
Simple test runner for LookML Text-to-SQL system.

This just runs a list of questions through main.py and captures:
- The question asked
- Schema/context found during grounding
- What gets sent to the LLM (if using LLM planner)
- The final SQL output
- Any errors that occur

Much simpler than the over-engineered testing framework.
"""

import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from src.main import TextToSQLEngine
import glob
import os

logger = logging.getLogger(__name__)

def load_test_questions(file_path: str = "simple_test_questions.txt") -> List[str]:
    """Load test questions from a simple text file."""
    questions = []
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if line and not line.startswith('#'):
                questions.append(line)
    
    return questions

def _extract_llm_conversation(engine, question: str, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract LLM conversation details from the engine or conversation logs."""
    try:
        # Method 1: Try to get from the most recent LLM response if available
        if hasattr(engine, 'query_planner') and hasattr(engine.query_planner, 'gemini_service'):
            gemini_service = engine.query_planner.gemini_service
            
            # Check if conversation logs are enabled
            if hasattr(gemini_service, 'conversation_log_dir') and gemini_service.conversation_log_dir:
                log_dir = gemini_service.conversation_log_dir
                
                # Find the most recent conversation log file
                pattern = os.path.join(log_dir, "conversation_*.json")
                log_files = glob.glob(pattern)
                
                if log_files:
                    # Get the most recent file
                    latest_log = max(log_files, key=os.path.getctime)
                    
                    try:
                        with open(latest_log, 'r') as f:
                            conversation_data = json.load(f)
                        
                        # Extract the key information
                        context = conversation_data.get('context', {})
                        response = conversation_data.get('response', {})
                        
                        return {
                            'conversation_id': conversation_data.get('conversation_id', 'unknown'),
                            'prompt_sent': context.get('user_prompt', ''),
                            'system_prompt': context.get('system_prompt', ''),
                            'schema_context': context.get('schema_context', ''),
                            'raw_response': response.get('content', ''),
                            'timestamp': conversation_data.get('timestamp', ''),
                            'total_context_size': context.get('total_size', 0)
                        }
                    except Exception as e:
                        logger.warning(f"Error reading conversation log {latest_log}: {e}")
        
        # Method 2: Try to access the LLM response directly from result if available
        # This would require modifications to the main engine to expose this data
        # For now, we'll rely on the conversation logs
        
        return None
        
    except Exception as e:
        logger.warning(f"Error extracting LLM conversation: {e}")
        return None

def run_simple_tests(config_path: Optional[str] = None, 
                    output_file: str = None) -> List[Dict[str, Any]]:
    """
    Run simple tests - just iterate through questions and capture everything.
    """
    
    if output_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"test_results_{timestamp}.json"
    
    print("Loading test questions...")
    questions = load_test_questions()
    print(f"Found {len(questions)} test questions")
    
    print("Initializing TextToSQL engine...")
    engine = TextToSQLEngine(config_path)
    
    results = []
    
    for i, question in enumerate(questions, 1):
        print(f"\n[{i}/{len(questions)}] Testing: {question}")
        
        start_time = time.time()
        
        try:
            # Get the result from engine
            result = engine.generate_sql(question)
            execution_time = time.time() - start_time
            
            # Capture what we care about
            test_result = {
                'question_number': i,
                'question': question,
                'execution_time': execution_time,
                'success': result.get('error') is None,
                'sql_output': result.get('sql'),
                'error_message': result.get('error'),
                
                # Schema/grounding info
                'explore_used': result.get('explore_used'),
                'fields_selected': result.get('fields_selected', []),
                'joins_required': result.get('joins_required', []),
                
                # LLM info (if available)
                'llm_used': result.get('llm_used', False),
                'llm_cost': result.get('llm_cost_estimate', 0.0),
                'llm_tokens': result.get('llm_token_usage', {}),
                
                # Other metadata
                'limit_applied': result.get('limit_applied', False),
                'validation_passed': result.get('validation_passed'),
                'processing_time': result.get('processing_time', execution_time)
            }
            
            # Try to get more detailed context if we can access the grounding index
            if hasattr(engine, 'grounding_index') and engine.grounding_index:
                try:
                    # Get available explores
                    available_explores = list(engine.grounding_index.explores.keys()) if hasattr(engine.grounding_index, 'explores') else []
                    test_result['available_explores'] = available_explores
                    
                    # Get available fields for the used explore
                    if result.get('explore_used') and hasattr(engine.grounding_index, 'explores'):
                        explore = engine.grounding_index.explores.get(result.get('explore_used'))
                        if explore:
                            available_fields = [f.qualified_name for f in explore.dimensions + explore.measures]
                            test_result['available_fields_in_explore'] = available_fields[:20]  # Limit to first 20
                
                except Exception as e:
                    test_result['context_extraction_error'] = str(e)
            
            # Try to capture LLM conversation details if available
            if test_result['llm_used']:
                try:
                    llm_conversation = _extract_llm_conversation(engine, question, result)
                    if llm_conversation:
                        test_result['llm_prompt_sent'] = llm_conversation['prompt_sent']
                        test_result['llm_raw_response'] = llm_conversation['raw_response']
                        test_result['llm_system_prompt'] = llm_conversation['system_prompt']
                        test_result['llm_schema_context'] = llm_conversation['schema_context']
                        test_result['llm_conversation_id'] = llm_conversation['conversation_id']
                except Exception as e:
                    test_result['llm_extraction_error'] = str(e)
            
            if test_result['success']:
                print(f"  ✓ Generated SQL ({execution_time:.2f}s)")
                if test_result['llm_used']:
                    print(f"    LLM cost: ${test_result['llm_cost']:.4f}")
                    if test_result.get('llm_prompt_sent'):
                        print(f"    LLM conversation captured ✓")
                    elif test_result.get('llm_extraction_error'):
                        print(f"    LLM conversation failed: {test_result['llm_extraction_error']}")
                print(f"    Explore: {test_result['explore_used']}")
                print(f"    Fields: {len(test_result['fields_selected'])} selected")
            else:
                print(f"  ✗ Failed: {test_result['error_message']}")
        
        except Exception as e:
            execution_time = time.time() - start_time
            test_result = {
                'question_number': i,
                'question': question,
                'execution_time': execution_time,
                'success': False,
                'sql_output': None,
                'error_message': f"Exception: {str(e)}",
                'exception_type': type(e).__name__
            }
            print(f"  ✗ Exception: {e}")
        
        results.append(test_result)
    
    # Save results
    print(f"\nSaving results to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_questions': len(questions),
            'successful_queries': sum(1 for r in results if r['success']),
            'failed_queries': sum(1 for r in results if not r['success']),
            'total_execution_time': sum(r['execution_time'] for r in results),
            'total_llm_cost': sum(r.get('llm_cost', 0) for r in results),
            'config_used': config_path,
            'results': results
        }, f, indent=2)
    
    return results

def print_summary(results: List[Dict[str, Any]]):
    """Print a simple summary of test results."""
    
    total = len(results)
    successful = sum(1 for r in results if r['success'])
    failed = total - successful
    
    total_time = sum(r['execution_time'] for r in results)
    total_cost = sum(r.get('llm_cost', 0) for r in results)
    
    print(f"\n{'='*50}")
    print(f"SIMPLE TEST RESULTS SUMMARY")
    print(f"{'='*50}")
    print(f"Total questions: {total}")
    print(f"Successful: {successful} ({successful/total*100:.1f}%)")
    print(f"Failed: {failed} ({failed/total*100:.1f}%)")
    print(f"Total time: {total_time:.2f}s")
    print(f"Average time: {total_time/total:.2f}s per query")
    
    if total_cost > 0:
        print(f"Total LLM cost: ${total_cost:.4f}")
        print(f"Average cost: ${total_cost/total:.6f} per query")
    
    # Show failures
    if failed > 0:
        print(f"\nFAILED QUERIES:")
        for result in results:
            if not result['success']:
                print(f"  {result['question_number']:2d}. {result['question']}")
                print(f"      Error: {result['error_message']}")
    
    # Show most expensive queries (if using LLM)
    llm_results = [r for r in results if r.get('llm_cost', 0) > 0]
    if llm_results:
        print(f"\nMOST EXPENSIVE LLM QUERIES:")
        sorted_by_cost = sorted(llm_results, key=lambda x: x.get('llm_cost', 0), reverse=True)
        for result in sorted_by_cost[:5]:
            print(f"  ${result['llm_cost']:.4f} - {result['question']}")

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple test runner for LookML Text-to-SQL")
    parser.add_argument("--config", "-c", help="Path to configuration file")
    parser.add_argument("--questions", "-q", default="simple_test_questions.txt", 
                       help="Path to questions file")
    parser.add_argument("--output", "-o", help="Output file for results (default: auto-generated)")
    
    args = parser.parse_args()
    
    try:
        # Update the questions file path if specified
        global questions_file
        questions_file = args.questions
        
        # Run tests
        results = run_simple_tests(
            config_path=args.config,
            output_file=args.output
        )
        
        # Print summary
        print_summary(results)
        
        print(f"\nDetailed results saved to JSON file")
        print("You can analyze the results to see:")
        print("- Which questions worked vs failed")
        print("- What schema/context was found for each")
        print("- LLM inputs/outputs (in conversation logs)")
        print("- Generated SQL for successful queries")
        
    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
        return 1
    except Exception as e:
        print(f"Test run failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())