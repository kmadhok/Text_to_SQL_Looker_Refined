# Enhanced Simple Testing with LLM Conversation Capture

Your simple testing framework now captures **the actual LLM conversations** - exactly what you asked for!

## 🎯 What's New

The enhanced `simple_test_runner.py` now captures:

### Original Data (unchanged)
- ✅ Questions and SQL outputs
- ✅ Success/failure status  
- ✅ LLM costs and token usage
- ✅ Execution times and schema usage

### NEW: Complete LLM Conversations
- ✅ **Full prompt sent to LLM** (including schema context)
- ✅ **System prompt** (instructions given to LLM)
- ✅ **Schema context** (what schema info was provided)
- ✅ **Raw LLM response** (before any post-processing)
- ✅ **Conversation ID** (links to detailed logs)

## 🚀 Quick Start

### 1. Run Enhanced Tests
```bash
# Make sure your config has LLM conversation logging enabled
python simple_test_runner.py
```

### 2. Analyze Results with LLM Details
```bash
# Basic analysis
python analyze_test_results.py test_results_20250903_123456.json

# Show more LLM conversation examples
python analyze_test_results.py test_results_*.json --llm-conversations 5
```

### 3. See What's New
```bash
# Demonstration of new capabilities
python test_enhanced_runner.py
```

## 📊 New Analysis Features

### LLM Conversation Analysis
The analyzer now shows:
- **Conversation capture rate**: How many LLM calls had full details captured
- **Prompt size analysis**: Average prompt sizes and schema context sizes
- **Sample conversations**: Full prompt → response → SQL chains
- **Conversation IDs**: Links to detailed log files

### Example Output
```
LLM CONVERSATION ANALYSIS:
------------------------------
Average prompt size: 2,847 characters
Average schema context size: 12,450 characters

Sample LLM conversations (showing first 3):

Example 1: Show me total revenue
  Query sent to LLM: Show me total revenue
  Schema context: 12,450 chars (e.g., "# Available Tables and Fields...")
  LLM response: SELECT sum(t1.sale_price) AS total_revenue FROM...

Conversation IDs captured: 33
Example conversation IDs: 1f973993_attempt_1, 3ca81714_attempt_1, 863081d3_attempt_1
```

## 🔍 What You Can Now Analyze

### Schema Context Effectiveness
- Which schema information leads to better SQL?
- Is too much context confusing the LLM?
- Are key relationships missing from the context?

### LLM Reasoning Patterns
- How does the LLM interpret ambiguous questions?
- What patterns lead to incorrect SQL generation?
- Which question types need better prompting?

### Prompt Engineering Insights
- Optimal system prompt adjustments
- Schema context size vs. quality trade-offs
- Question-specific guidance effectiveness

## 📁 Enhanced JSON Output

Each test result now includes:
```json
{
  "question": "Show me total revenue",
  "success": true,
  "sql_output": "SELECT SUM(sale_price) FROM...",
  
  "llm_prompt_sent": "Database Schema:\n...\nNatural Language Query: Show me total revenue",
  "llm_system_prompt": "You are an expert SQL developer...",
  "llm_schema_context": "# Available Tables\n## order_items\n- sale_price...",
  "llm_raw_response": "SELECT\n  sum(t1.sale_price) AS total_revenue\nFROM...",
  "llm_conversation_id": "1f973993_attempt_1",
  
  "llm_cost": 0.00933,
  "llm_tokens": {"prompt_tokens": 2328, "completion_tokens": 40}
}
```

## 🔧 Configuration Requirements

For LLM conversation capture to work:

### In your `config.yaml`:
```yaml
llm:
  save_conversations: true
  conversation_log_dir: "data/llm_logs"
  
generator:
  use_llm_planner: true
```

### Directory Structure:
```
your_project/
├── simple_test_runner.py      # Enhanced test runner
├── analyze_test_results.py    # Enhanced analyzer  
├── data/
│   └── llm_logs/              # LLM conversation logs
│       ├── conversation_1f973993_attempt_1.json
│       └── conversation_3ca81714_attempt_1.json
└── test_results_20250903_123456.json  # Enhanced results
```

## 🧠 Analysis Insights You'll Get

Real examples of what you can now discover:

### "The LLM is missing key context"
```
Schema context: 8,450 chars
LLM response: "I need to join orders and order_items but don't see the relationship"
→ Add foreign key information to schema context
```

### "Ambiguous questions confuse the LLM"
```
Question: "Show me the best customers"
LLM response: "I'm not sure what 'best' means - highest revenue? most orders?"
→ Add guidance for handling ambiguous metrics
```

### "Long prompts hurt performance"
```
Average success rate:
- Prompts < 3000 chars: 95%
- Prompts > 5000 chars: 78%
→ Optimize schema context size
```

## 🔄 How It Works

1. **Test Runner** calls your `TextToSQLEngine`
2. **Engine** uses `LLMQueryPlanner` → `GeminiService`
3. **GeminiService** saves conversation to log file
4. **Test Runner** reads most recent log file
5. **Extracts** prompt/response data into test results
6. **Analyzer** shows conversation patterns and insights

## 🚨 Troubleshooting

### "No LLM conversations captured"
- Check `save_conversations: true` in config
- Verify `conversation_log_dir` exists
- Ensure `use_llm_planner: true`

### "LLM extraction errors"
- Check file permissions in conversation log directory
- Verify JSON format in log files
- Look for `llm_extraction_error` field in results

### "Empty conversation data"
- Log files may be getting overwritten too quickly
- Try running fewer questions at once
- Check file timestamps in log directory

## 💡 Next Steps

Now that you have full LLM conversation visibility:

1. **Run the enhanced tests** on your question set
2. **Analyze the conversation patterns** to identify issues
3. **Optimize your schema context** based on findings
4. **Adjust system prompts** for better performance
5. **Iterate and improve** your LLM-powered SQL generation

The simple approach you wanted, but now with complete visibility into what's happening under the hood! 🎉