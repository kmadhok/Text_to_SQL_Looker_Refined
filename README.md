# LookML-Grounded Text-to-SQL

A single-turn, command-style Text-to-SQL assistant that generates minimal BigQuery Standard SQL for e-commerce datasets. The system is grounded in LookML (views, explores, model) and validated against BigQuery metadata.

## Features

- **LookML-Grounded**: Only uses fields and joins defined in LookML explores
- **BigQuery Integration**: Validates against BigQuery `INFORMATION_SCHEMA` and enriches with column descriptions
- **Minimal SQL Output**: Generates clean, executable SQL without comments
- **Automatic LIMIT**: Enforces `LIMIT 100` when user doesn't specify a limit
- **Field Mapping**: Resolves LookML expressions like `${TABLE}` and `${view.field}`
- **Caching**: In-memory and file-based caching for metadata
- **Dry-run Validation**: Optional BigQuery dry-run validation for generated SQL

## Installation

### Prerequisites

- Python 3.11+
- Access to BigQuery (for metadata loading and optional validation)
- LookML files (locally cloned repository)

### Setup

1. **Clone and setup virtual environment**:
```bash
git clone <repository-url>
cd V2_text_to_sql_looker

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

2. **Configuration**:
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your settings
# GOOGLE_CLOUD_PROJECT=your-project-id
# BIGQUERY_DATASET=bigquery-public-data.thelook_ecommerce
# LOOKML_REPO_PATH=./data/lookml
```

3. **Setup LookML data**:
```bash
# Create directory for LookML files
mkdir -p data/lookml

# Download sample LookML files or clone your LookML repository
# For testing, you can use the Looker sample project:
# https://github.com/looker/ecommerce
```

## Usage

### Command Line Interface

**Interactive mode**:
```bash
python -m src.main --interactive
```

**Single query**:
```bash
python -m src.main --query "average order value by device in the last 30 days"
```

**With validation**:
```bash
python -m src.main --query "show top 10 customers by revenue" --validate
```

**Verbose output**:
```bash
python -m src.main --query "count of orders by status" --verbose
```

### Python API

```python
from src.main import TextToSQLEngine

# Initialize engine
engine = TextToSQLEngine()

# Generate SQL
result = engine.generate_sql("average order value by device")

if result['error']:
    print(f"Error: {result['error']}")
else:
    print(result['sql'])
    print(f"Used explore: {result['explore_used']}")
    print(f"Selected fields: {result['fields_selected']}")
```

## Configuration

The system uses YAML configuration with environment variable overrides:

### config/config.yaml
```yaml
bigquery:
  project_id: null  # Set via GOOGLE_CLOUD_PROJECT env var
  dataset: "bigquery-public-data.thelook_ecommerce"
  location: "US"

lookml:
  repo_path: "./data/lookml"

cache:
  enabled: true
  directory: "./data/cache"
  metadata_ttl: 3600

generator:
  default_limit: 100
  enable_dry_run: false
  max_joins: 10
```

### Environment Variables
- `GOOGLE_CLOUD_PROJECT`: Your BigQuery project ID
- `BIGQUERY_DATASET`: Target dataset (default: bigquery-public-data.thelook_ecommerce)
- `LOOKML_REPO_PATH`: Path to LookML files
- `ENABLE_DRY_RUN`: Enable SQL validation (true/false)
- `DEFAULT_LIMIT`: Default row limit for queries

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   LookML Files  │───▶│   LookML Parser  │───▶│ Grounding Index │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐              │
│ BigQuery Schema │───▶│ Metadata Loader  │─────────────┘
└─────────────────┘    └──────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐    ┌─────────▼─────────┐
│ Natural Language│───▶│  Query Planner   │───▶│   SQL Builder    │
└─────────────────┘    └──────────────────┘    └─────────┬─────────┘
                                                         │
                       ┌──────────────────┐    ┌─────────▼─────────┐
                       │   SQL Validator  │◀───│  Generated SQL   │
                       └──────────────────┘    └───────────────────┘
```

### Core Components

1. **LookML Parser**: Parses `.lkml` files into structured data models
2. **Metadata Loader**: Fetches BigQuery schema and column descriptions
3. **Grounding Index**: Combines LookML and BigQuery metadata for field resolution
4. **Query Planner**: Selects appropriate explore and fields based on natural language input
5. **SQL Builder**: Generates minimal BigQuery SQL with proper joins and expressions
6. **Validator**: Optional dry-run validation against BigQuery

## Examples

**Input**: "average order value by device in the last 30 days"

**Output**:
```sql
SELECT
  users.traffic_source AS traffic_source,
  AVG(order_items.sale_price) AS average_order_value
FROM `bigquery-public-data.thelook_ecommerce.orders` AS orders
LEFT JOIN `bigquery-public-data.thelook_ecommerce.users` AS users 
  ON orders.user_id = users.id
LEFT JOIN `bigquery-public-data.thelook_ecommerce.order_items` AS order_items 
  ON orders.id = order_items.order_id
WHERE DATE_DIFF(CURRENT_DATE(), DATE(orders.created_at), DAY) <= 30
GROUP BY 1
LIMIT 100
```

## Testing

Run the test suite:

```bash
# Install development dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Development

### Project Structure
```
├── src/
│   ├── lookml/          # LookML parsing
│   ├── bigquery/        # BigQuery client and metadata
│   ├── grounding/       # Field mapping and grounding index
│   ├── generator/       # Query planning and SQL generation
│   ├── utils/           # Utilities (caching, etc.)
│   ├── config.py        # Configuration management
│   └── main.py          # CLI and main engine
├── tests/               # Test suite
├── config/              # Configuration files
└── data/                # Data directory (LookML, cache)
```

### Adding New Features

1. **Extend LookML Support**: Add new LookML constructs in `src/lookml/models.py`
2. **Improve Field Resolution**: Enhance `src/grounding/field_mapper.py`
3. **Add Query Patterns**: Extend `src/generator/planner.py` for new query types
4. **Custom SQL Functions**: Modify `src/generator/sql_builder.py`

## Limitations

- **Single-turn queries only**: No multi-turn conversations
- **Basic filter support**: Limited natural language filter parsing
- **No row-level security**: Does not emulate Looker access controls
- **Simple expression resolution**: Complex PDTs and derived tables not fully supported

## Performance

- **Target latency**: p50 < 5 seconds (with cached metadata)
- **Metadata caching**: In-memory + file-based caching with TTL
- **Query validation**: Optional dry-run validation (adds ~1-2 seconds)

## Troubleshooting

### Common Issues

1. **"Could not select appropriate explore"**
   - Check that LookML files are properly parsed
   - Ensure query terms match field names or descriptions

2. **"Table not found" errors**
   - Verify BigQuery dataset access
   - Check that LookML `sql_table_name` references match actual tables

3. **"Permission denied" errors**
   - Ensure proper BigQuery IAM permissions
   - Verify project ID and dataset configuration

### Debug Mode

Enable verbose logging:
```bash
python -m src.main --query "your query" --verbose
```

This shows detailed information about:
- Explore selection process
- Field matching scores
- Generated SQL structure
- Validation results