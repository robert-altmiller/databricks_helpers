# Tag & Comment Framework - Class-Based Implementation

A modular, class-based framework for managing Unity Catalog tags, table descriptions, and column descriptions using AI-powered generation.

## Project Structure

```
tag_comment_framework_util/
├── utils/
│   ├── __init__.py                        # Package initialization
│   ├── tag_processor.py                   # Tag application logic
│   ├── column_description_processor.py    # Column description generation & import
│   └── table_description_processor.py     # Table description generation & import
├── tags_comments_analysis.py              # Main orchestration notebook
├── requirements.txt                        # Python dependencies
└── README.md                              # Documentation
```

## Features

- **Modular Design**: Reusable Python classes for each operation
- **Multithreaded Processing**: Parallel execution for improved performance
- **AI-Powered**: Leverages Databricks AI endpoints for intelligent descriptions
- **Error Handling**: Clear error messages and failure tracking
- **Progress Tracking**: Real-time status updates during processing

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Import Classes

```python
from utils import (
    TagProcessor,
    ColumnDescriptionGenerator,
    ColumnDescriptionImporter,
    TableDescriptionGenerator,
    TableDescriptionImporter
)
```

### 3. Run the Main Notebook

Open `tags_comments_analysis.py` in Databricks and configure:
- **file_path**: Path to Excel file with table metadata
- **base_volume_path**: Volume path for storing output files
- **model_endpoint**: AI model to use for description generation

## Usage Examples

### Apply Tags

```python
tag_processor = TagProcessor(
    spark=spark,
    tag_column_mapping={"tag_domain": "domain", "tag_project": "project"},
    verbose=False
)
results = tag_processor.execute(df_pandas, max_workers=10)
```

### Generate Column Descriptions

```python
generator = ColumnDescriptionGenerator(
    spark=spark,
    catalog="catalog_name",
    schema="schema_name",
    table="table_name",
    output_path="/Volumes/path/",
    dbutils=dbutils,
    endpoint_name="databricks-meta-llama-3-3-70b-instruct",
    verbose=False
)
descriptions = generator.execute()
generator.save_comments(descriptions)
```

### Import Column Descriptions

```python
importer = ColumnDescriptionImporter(
    spark=spark,
    input_path="/Volumes/path/bulk_comments/columns/json",
    verbose=False
)
importer.execute()
```

### Generate Table Descriptions

```python
generator = TableDescriptionGenerator(
    spark=spark,
    catalog="catalog_name",
    schema="schema_name",
    table="table_name",
    output_path="/Volumes/path/",
    dbutils=dbutils,
    endpoint_name="databricks-meta-llama-3-3-70b-instruct",
    verbose=False
)
descriptions = generator.execute()
generator.save_descriptions(descriptions)
```

### Import Table Descriptions

```python
importer = TableDescriptionImporter(
    spark=spark,
    input_path="/Volumes/path/bulk_comments/tables/json",
    verbose=False
)
importer.execute()
```

## Classes Overview

### TagProcessor
**Purpose**: Apply tags to Unity Catalog tables

**Features**:
- Handles boolean conversions (1/0 → true/false)
- Skips NA/null values
- Catches policy violations
- Returns detailed success/failure status

**Key Parameters**:
- `tag_column_mapping`: Dictionary mapping Excel columns to tag names
- `verbose`: Enable detailed logging (default: False)
- `max_workers`: Number of parallel threads

### ColumnDescriptionGenerator
**Purpose**: Generate AI-powered descriptions for table columns

**Features**:
- Fetches column metadata from information_schema
- Retrieves sample data for context
- Generates descriptions using AI
- Saves results to storage

**Key Parameters**:
- `endpoint_name`: AI model endpoint
- `data_limit`: Number of sample rows (default: 5)
- `max_cell_chars`: Max characters per cell (default: 1000)
- `prompt_return_length`: Max words in description (default: 40)
- `always_update`: Overwrite existing comments (default: True)

### ColumnDescriptionImporter
**Purpose**: Apply generated column descriptions to Unity Catalog

**Features**:
- Reads descriptions from storage
- Groups by table for efficient processing
- Updates Unity Catalog with ALTER TABLE commands
- Tracks success/failure for each column

### TableDescriptionGenerator
**Purpose**: Generate AI-powered descriptions for tables

**Features**:
- Fetches table schema and metadata
- Includes sample data for context
- Generates comprehensive table descriptions
- Saves results to storage

**Key Parameters**:
- `endpoint_name`: AI model endpoint
- `data_limit`: Number of sample rows (default: 2)
- `max_cell_chars`: Max characters per cell (default: 1000)
- `prompt_return_length`: Max words in description (default: 200)
- `always_update`: Overwrite existing comments (default: True)

### TableDescriptionImporter
**Purpose**: Apply generated table descriptions to Unity Catalog

**Features**:
- Reads descriptions from storage
- Batch processing for multiple tables
- Updates Unity Catalog with COMMENT ON TABLE commands
- Returns count of processed tables

## Configuration

### AI Model Endpoints

The framework supports multiple AI models:
- `databricks-meta-llama-3-3-70b-instruct` (default)
- `databricks-claude-sonnet-4-5`
- `databricks-gemini-2-5-pro`

### Storage Format

Generated descriptions are saved in JSON format (CSV also supported) to Unity Catalog Volumes:
```
/Volumes/<catalog>/<schema>/<volume>/bulk_comments/
├── columns/
│   └── json/
│       ├── table1/
│       └── table2/
└── tables/
    └── json/
        ├── table1/
        └── table2/
```

### Parallelism

The framework automatically calculates optimal parallelism:
```python
default_parallelism = spark.sparkContext.defaultParallelism / 2
# Fallback: (4 * os.cpu_count()) - 1
```

You can override this when calling `execute()`:
```python
processor.execute(df, max_workers=15)
```

## Error Handling

The framework provides clear error messages for common issues:

- **Table not found**: "Table not found: catalog.schema.table"
- **Permission denied**: "Permission denied on /Volumes/..."
- **AI endpoint failure**: "AI endpoint unavailable"
- **Tag policy violation**: "Invalid tag value for policy"
- **Save failure**: "Save failed: PathIOException"

All errors are logged and included in the final execution report.

## Best Practices

1. **Use verbose=False**: When running multithreaded operations, disable verbose logging for cleaner output
2. **Pass dbutils**: Always pass dbutils to generators for proper file cleanup
3. **Review results**: Check the detailed status report for any failures
4. **Adjust parallelism**: Set max_workers based on cluster size and workload
5. **Monitor AI costs**: Be mindful of the number of AI calls when processing large datasets

## Dependencies

Required Python packages (see `requirements.txt`):
```
openpyxl  # For reading Excel files
```

PySpark and Databricks utilities are provided by the Databricks runtime.

## Workflow

The main notebook follows this workflow:

1. **Load Excel** → Read table metadata from Excel file
2. **Apply Tags** → Apply tags to tables using TagProcessor
3. **Identify Tables** → Determine which tables need descriptions
4. **Column Descriptions** → Generate and import column descriptions (multithreaded)
5. **Table Descriptions** → Generate and import table descriptions (multithreaded)
6. **Final Report** → Display execution summary with success/failure counts

## Troubleshooting

### Issue: "PathIOException: Input/output error"
**Solution**: The framework automatically cleans old files before saving. Ensure dbutils is passed to generators.

### Issue: "DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN"
**Solution**: This has been fixed in the current version. Ensure you're using the latest code.

### Issue: "Table not found in system.information_schema"
**Solution**: Verify the table exists and you have permissions to access it.

### Issue: "AI endpoint unavailable"
**Solution**: Check that the model endpoint name is correct and the endpoint is running.

## License

Internal use only.
