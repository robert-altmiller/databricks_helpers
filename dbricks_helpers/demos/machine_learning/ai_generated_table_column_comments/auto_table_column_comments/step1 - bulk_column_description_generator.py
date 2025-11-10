# Databricks notebook source
# DBTITLE 1,Library Imports
import os
from typing import List, Tuple
from pyspark.sql import functions as F
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Set up Databricks Widgets
dbutils.widgets.text("Catalog", "", "Enter Catalog Name (Mandatory):")
dbutils.widgets.text("Schema", "", "Enter Schema Name (Optional):")
dbutils.widgets.text("Table", "", "Enter Table Name (Optional):")
dbutils.widgets.text("Output Path", "", "Enter Base Volumes Path (Mandatory):")
dbutils.widgets.text("Model Serving Endpoint Name", "databricks-meta-llama-3-3-70b-instruct", "Model Serving Endpoint Name (Mandatory):")
dbutils.widgets.text("Sample Data Limit", "5", "Sample Data Limit (Mandatory):")
dbutils.widgets.text("Sample Max Cell Chars", "1000", "Sample Max Cell Chars (Mandatory):")
dbutils.widgets.dropdown("Always Update Comments", choices=["true", "false"], defaultValue="true", label="Always Update Comments (Optional):")
dbutils.widgets.dropdown("Prompt Return Length", choices=["10", "20", "30", "40", "50"], defaultValue="40", label="Prompt Return Length (Mandatory):")

# COMMAND ----------

# DBTITLE 1,Local Parameters
# UC table name (mandatory)
catalog = dbutils.widgets.get("Catalog")
print(f"catalog: {catalog}")

# UC schema name (optional)
schema = dbutils.widgets.get("Schema")
print(f"schema: {schema}")

# UC table name (optional)
table = dbutils.widgets.get("Table")
print(f"table: {table}")

# UC Volumes path to store the output column comment results
output_path = dbutils.widgets.get("Output Path") + "/bulk_comments/columns"
print(f"output_path: {output_path}")

# Databricks LLM model serving endpoint
endpoint_name = dbutils.widgets.get("Model Serving Endpoint Name")
print(f"endpoint_name: {endpoint_name}")

# Sample Data Limit specifies how many rows of data to return per table
data_limit = int(dbutils.widgets.get("Sample Data Limit"))
print(f"data_limit: {data_limit}")

# Sample Max Cell Chars specifies how many character to return per row-col.
max_cell_chars = int(dbutils.widgets.get("Sample Max Cell Chars"))
print(f"max_cell_chars: {max_cell_chars}")

# Prompt return table column comment length
prompt_return_length = int(dbutils.widgets.get("Prompt Return Length"))
print(f"prompt_return_length: {prompt_return_length}")

# Overwrite AI-generated table column comments
always_update = dbutils.widgets.get("Always Update Comments").lower() == "true"
print(f"always_update: {always_update}")

# Output file format (e.g., csv or json)
output_file_format = "json" # or csv
print(f"output_file_format: {output_file_format}")

# Calculate default parallelism
try: default_parallelism = spark.sparkContext.defaultParallelism / 2 # Does not work with serverless
except: default_parallelism = (4 * os.cpu_count()) - 1
print(f"Number of executors: {default_parallelism}")

# COMMAND ----------

# DBTITLE 1,Get Sample Data (Multi-Threaded)
def get_sample_data(catalog: str, limit: int, schema: str = None, table: str = None, max_workers: int = 8, max_cell_chars: int = 1000) -> dict:
    """
    Fetch sample rows from one or more tables within a catalog (and optionally schema/table),
    using multi-threading for faster parallel execution.
    Truncates only *individual column values* that exceed a safe character limit to prevent
    oversized AI prompts (e.g., large array or JSON string columns).
    Args:
        catalog (str): The catalog name to query.
        limit (int): Maximum number of rows to fetch from each table.
        schema (str, optional): Restrict search to a single schema if provided.
        table (str, optional): Restrict search to a single table if provided.
        max_workers (int, optional): Number of threads for parallel fetching. Default = 8.
        max_cell_chars (int, optional): Maximum number of characters allowed per cell value. 
            Default = 1000. Long individual cell values are truncated to this length 
            to avoid exceeding AI model token limits.
    Returns:
        dict: Dictionary keyed by (catalog, schema, table) with values as:
              - List of row dictionaries containing truncated sample data if successful.
              - List with a single {"error": "..."} dictionary if data retrieval failed.
    """

    sample_data = {}
    query = f"""
        SELECT table_catalog, table_schema, table_name
        FROM system.information_schema.tables
        WHERE table_catalog = :catalog
    """
    if schema:
        query += " AND table_schema = :schema"
        if table:
            query += " AND table_name = :table"

    tables = spark.sql(
        query,
        args={"catalog": catalog, "schema": schema, "table": table}
    ).collect()

    # ------------------------------------------------------------------
    # Worker function to fetch data for a single table.
    # Each individual column value is truncated if it exceeds `max_cell_chars`.
    # ------------------------------------------------------------------
    def fetch_table_data(t):
        full_table_name = f"{t.table_catalog}.{t.table_schema}.{t.table_name}"
        data_query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

        try:
            rows_raw = [row.asDict(recursive=True) for row in spark.sql(data_query).collect()]
            rows_clean = []

            for row_dict in rows_raw:
                clean_row = {}
                for col, val in row_dict.items():
                    if val is None:
                        clean_row[col] = None
                    else:
                        val_str = str(val)
                        # Truncate overly long individual column values only
                        if len(val_str) > max_cell_chars:
                            clean_row[col] = val_str[:max_cell_chars] + " ...[truncated]"
                        else:
                            clean_row[col] = val_str
                rows_clean.append(clean_row)

            return (t.table_catalog, t.table_schema, t.table_name), rows_clean

        except Exception as e:
            # Handle query or permission errors gracefully
            return (t.table_catalog, t.table_schema, t.table_name), [{"error": str(e)}]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {executor.submit(fetch_table_data, t): t for t in tables}
        for future in as_completed(future_to_table):
            key, rows = future.result()
            sample_data[key] = rows

    return sample_data


# Get table metadata with a few sample rows based on data_limit and max cell chars.
# sample_data = get_sample_data(catalog="gdp_stage", limit=3, schema="allocation", table=table, max_workers=default_parallelism, max_cell_chars=max_cell_chars)
# print(sample_data)

# COMMAND ----------

# DBTITLE 1,Get Column Comments (Multi-Threaded)
def describe_single_column(col: Row, samples: dict, endpoint_name: str, replace_comment: bool, prompt_return_length: int) -> Row:
    """
    Generate an AI-assisted description for a single column.
    Args:
        col (Row): A Spark Row containing column metadata from information_schema.columns,
                   including table_catalog, table_schema, table_name, column_name, data_type,
                   ordinal_position, and existing_comment.
        samples (dict): Dictionary keyed by (catalog, schema, table) with lists of sample rows
                        (from get_sample_data). Used to provide context for column description.
        endpoint_name (str): The registered AI endpoint name used for generating the description
                             via the ai_query function.
        replace_comment: Whether to replace the existing comment with the new one.
        prompt_return_length (int): Maximum number of words in the AI-generated description.
    Returns:
        Row: A Spark Row containing:
             - table_catalog (str): Catalog the column belongs to.
             - table_schema (str): Schema the column belongs to.
             - table_name (str): Table the column belongs to.
             - column_name (str): Name of the column.
             - ordinal_position (int): Position of the column within the table.
             - existing_comment (str): Existing column comment (if any).
             - replace_comment (bool): Whether the existing comment is missing/empty.
             - new_comment (str): Newly generated one-sentence description of the column.
    """
    # Skip AI generation if always_update is False and column already has a comment
    should_generate = replace_comment or col.replace_comment
    if not should_generate:
        return Row(
            table_catalog=col.table_catalog,
            table_schema=col.table_schema,
            table_name=col.table_name,
            column_name=col.column_name,
            ordinal_position=col.ordinal_position,
            existing_comment=col.existing_comment,
            replace_comment=False,
            new_comment=None
        )

    key = (col.table_catalog, col.table_schema, col.table_name)
    sample_rows = samples.get(key, [])
    sample_text = str(sample_rows[:3])  # Use only first 3 rows for brevity

    prompt = (
        f'This description will be stored as a Unity Catalog table column comment. '
        f'Write a detailed, single-sentence description of approximately {prompt_return_length} words '
        f'for the column "{col.column_name}" from the table "{col.table_name}" in schema "{col.table_schema}" '
        f'within catalog "{col.table_catalog}". '
        f'This description should clearly explain what kind of information the column contains and its purpose. '
        f'The column data type is "{col.data_type}". '
        f'Use the following sample rows for context: {sample_text}. '
        f'Keep the description professional and concise, suitable for a data dictionary. '
        f'Do not mention schema or catalog names in the output.'
    )
    ai_sql = f"SELECT ai_query('{endpoint_name}', :prompt) AS new_comment"
    new_comment = spark.sql(ai_sql, args={"prompt": prompt}).collect()[0].new_comment
    
    if not replace_comment:
        replace_comment = col.replace_comment
    
    return Row(
        table_catalog=col.table_catalog,
        table_schema=col.table_schema,
        table_name=col.table_name,
        column_name=col.column_name,
        ordinal_position=col.ordinal_position,
        existing_comment=col.existing_comment,
        replace_comment=replace_comment,
        new_comment=new_comment
    )


def get_column_comments(
    catalog: str, limit: int, schema: str = None, 
    table: str = None, max_workers: int = 8, replace_comment: bool = False, 
    prompt_return_length: int = 40, max_cell_chars: int = 1000
) -> DataFrame:
    """
    Generate AI-assisted column descriptions for all columns in a catalog/schema/table.
    This function:
      1. Fetches sample rows per table (via get_sample_data).
      2. Retrieves column metadata from information_schema.columns.
      3. Uses multi-threading to generate column descriptions in parallel
         by calling `_describe_column` for each column.
      4. Returns a Spark DataFrame with both metadata and AI-generated descriptions.
    Args:
        catalog (str): The catalog name to scan for tables/columns.
        limit (int): Number of sample rows per table to include for description context.
        schema (str, optional): Restrict to a single schema. Defaults to "" (all schemas).
        table (str, optional): Restrict to a single table. Defaults to "" (all tables).
        max_workers (int, optional): Number of threads for parallel execution. Defaults to 8.
        prompt_return_length (int): Maximum number of words in the AI-generated description.
        max_cell_chars (int): Maximum number of characters in each cell of the sample rows.
    Returns:
        DataFrame: A Spark DataFrame with the following schema:
            - table_catalog (str)
            - table_schema (str)
            - table_name (str)
            - column_name (str)
            - ordinal_position (int)
            - existing_comment (str)
            - replace_comment (bool)
            - new_comment (str)
    """
    # Step 1: get sample rows
    samples = get_sample_data(catalog, limit, schema, table, max_workers, max_cell_chars)

    # Step 2: get column metadata
    query = f"""
      SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.data_type,
             c.ordinal_position,
             c.comment IS NULL or length(c.comment) == 0 AS replace_comment,
             c.comment AS existing_comment
      FROM system.information_schema.columns AS c
      JOIN system.information_schema.tables AS t 
           USING (table_catalog, table_schema, table_name)
      WHERE c.table_catalog = :catalog
    """
    if schema:
        query += " AND c.table_schema = :schema"
        if table:
            query += " AND c.table_name = :table"
    query += " ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position"

    columns = spark.sql(query, args={"catalog": catalog, "schema": schema, "table": table}).collect()

    # Step 3: Parallelize description calls
    rows_out = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(describe_single_column, col, samples, endpoint_name, replace_comment, prompt_return_length) for col in columns]
        for f in as_completed(futures):
            rows_out.append(f.result())

    # Step 4: Schema + DataFrame
    schema_out = StructType([
        StructField("table_catalog", StringType(), True),
        StructField("table_schema", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("ordinal_position", StringType(), True),
        StructField("existing_comment", StringType(), True),
        StructField("replace_comment", StringType(), True),
        StructField("new_comment", StringType(), True)
    ])
    df_out = spark.createDataFrame(rows_out, schema=schema_out)
    return df_out


# Get Column Comments
commented_columns = get_column_comments(
    catalog, data_limit, schema,
    table, max_workers=default_parallelism,
    replace_comment=always_update,
    prompt_return_length=prompt_return_length,
    max_cell_chars=max_cell_chars
)
display(commented_columns)

# COMMAND ----------

# DBTITLE 1,Add and Apply Column Comment User Updates
# -----> IMPORTANT <-----
# Pause here and review the table column comments in the output DF above.  
# Run this cell below to update AI generated column comments with your own comments.
# -----> IMPORTANT <-----

def apply_comment_updates(commented_columns: DataFrame, updates: List[Tuple[str, str, str, str, str, bool]]) -> DataFrame:
    """
    Apply manual updates to column comments in a Spark DataFrame.
    Args:
        commented_columns (DataFrame): A Spark DataFrame containing column metadata and 
            comments. Must include the following columns:
                - table_catalog (str)
                - table_schema (str)
                - table_name (str)
                - column_name (str)
                - new_comment (str)
                - replace_comment (bool)
        updates (list[tuple]): A list of tuples specifying manual updates. 
            Each tuple must have the form:
                (table_catalog, table_schema, table_name, column_name, updated_comment, replace_comment_update)
            - updated_comment (str): The new comment to apply to this column.
            - replace_comment_update (bool): If True, apply the update. If False, leave as-is.
    Returns:
        DataFrame: A Spark DataFrame with updated `new_comment` and `replace_comment` values. 
                   If `updates` is empty, the original DataFrame is returned unchanged.
    """
    if not updates:
        return commented_columns

    # Create a DataFrame with the updates
    updates_df = spark.createDataFrame(
        updates,
        ["table_catalog", "table_schema", "table_name", "column_name", 
         "updated_comment", "replace_comment_update"]
    )

    # Normalize replace_comment to boolean
    commented_columns = commented_columns.withColumn(
        "replace_comment", F.col("replace_comment").cast("boolean")
    )

    # Join and apply conditional updates
    commented_columns_updated = (
        commented_columns
        .join(
            updates_df,
            on=["table_catalog", "table_schema", "table_name", "column_name"],
            how="left"
        )
        .withColumn(
            "new_comment",
            F.when(
                (F.col("replace_comment_update") == F.lit(True)) & (F.col("updated_comment").isNotNull()),
                F.col("updated_comment")
            ).otherwise(F.col("new_comment"))
        )
        .withColumn(
            "replace_comment",
            F.when(F.col("replace_comment_update") == F.lit(True), F.lit(True))
             .otherwise(F.col("replace_comment"))
        )
        .drop("updated_comment", "replace_comment_update")
    )

    return commented_columns_updated



# Define your updates as a list of tuples - (catalog, schema, table, column, updated comment, replace_flag)
replace_flag = False # Change This to True if you want to replace the AI generated comment with your own
column_updates = [
    (catalog, schema, table, "acquisition_source_id", "This is the updated comment for acquisition_source_id.", replace_flag),
    (catalog, schema, table, "brand", "This is the updated comment for brand.", replace_flag),
]


# Update AI generated column comments with your own manual updates
commented_columns_updated = apply_comment_updates(commented_columns, column_updates).dropDuplicates()
display(commented_columns_updated)

# COMMAND ----------

# DBTITLE 1,Write AI Generated Column Comments to UC Volume
(
    commented_columns_updated
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .format(output_file_format)
    .save(output_path + f"/{output_file_format}")
)