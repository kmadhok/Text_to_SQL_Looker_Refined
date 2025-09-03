# Simple Testing for LookML Text-to-SQL

You're absolutely right - the testing should be simple! Here's a straightforward approach to test your system.

## What We Have

**`simple_test_questions.txt`** - Just a list of questions to test:
```
Show me total revenue
Count all users
What's the average order value?
Show me revenue by product category
...
```

**`simple_test_runner.py`** - Runs each question through your `main.py` and captures:
- The question asked
- Whether it succeeded or failed  
- The SQL generated
- What schema/context was found (explores, fields, joins)
- LLM usage and costs (if using LLM planner)
- Execution time

**`analyze_test_results.py`** - Analyzes the results to show you:
- Which questions worked vs failed
- What schema elements are being used
- Performance patterns
- LLM costs and usage

## How to Use

### 1. Run the simple tests
```bash
# Basic run
python simple_test_runner.py

# With custom config
python simple_test_runner.py --config config/config.yaml

# Custom questions file
python simple_test_runner.py --questions my_questions.txt
```

This creates a JSON file like `test_results_20240115_143022.json` with all the details.

### 2. Analyze the results
```bash
# Analyze the results  
python analyze_test_results.py test_results_20240115_143022.json

# Show more SQL samples
python analyze_test_results.py test_results_20240115_143022.json --samples 5
```

### 3. Look at what happened

The analysis shows you:
- **Success/failure patterns** - which types of questions work
- **Schema usage** - what explores and fields are being found
- **LLM analysis** - costs, token usage, expensive queries
- **Performance** - slow queries, average times
- **Sample SQL outputs** - see what actually got generated

## What You'll See

The JSON output captures everything:

```json
{
  "results": [
    {
      "question": "Show me total revenue",
      "success": true,
      "sql_output": "SELECT SUM(order_items.sale_price) as total_revenue FROM order_items...",
      "explore_used": "order_items", 
      "fields_selected": ["order_items.sale_price"],
      "joins_required": [],
      "llm_used": true,
      "llm_cost": 0.0023,
      "execution_time": 2.1
    },
    {
      "question": "Calculate average of customer names",
      "success": false,
      "error_message": "Cannot aggregate text field mathematically",
      "execution_time": 0.8
    }
  ]
}
```

## Adding More Questions

Just edit `simple_test_questions.txt`:
```
# Add your questions here - one per line
# Comments start with #

Your new question here?
Another question to test
```

## LLM Conversation Logs

If you have `save_conversations: true` in your config, you'll also get detailed LLM conversation logs in your `conversation_log_dir` showing exactly what context was sent to the LLM and what it responded with.

## That's It!

No complex test frameworks, no over-engineering. Just:
1. List of questions
2. Run them through your system  
3. See what worked and what didn't
4. Analyze the patterns

Much simpler and actually useful for understanding your system's behavior!