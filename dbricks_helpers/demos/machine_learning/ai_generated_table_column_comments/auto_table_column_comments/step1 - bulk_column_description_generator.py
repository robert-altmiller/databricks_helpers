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
dbutils.widgets.dropdown("Always Update Comments", choices=["true", "false"], defaultValue="true", label="Always Update Comments (Optional):")

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

# Overwrite AI-generated table descriptions
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
def get_sample_data(catalog: str, limit, schema: str = None, table: str = None, max_workers: int = 8) -> dict:
    """
    Fetch sample rows from one or more tables within a catalog (and optionally schema/table),
    using multi-threading for faster parallel execution.
    Args:
        catalog (str): The name of the catalog to query.
        limit (int): Maximum number of rows to fetch from each table.
        schema (str, optional): If provided, restricts search to a single schema. Defaults to "" (all schemas).
        table (str, optional): If provided along with schema, restricts search to a single table. Defaults to "".
        max_workers (int): Number of threads to use for parallel fetching. Defaults to 8.
    Returns:
        dict: Dictionary keyed by (catalog, schema, table) with values as:
              - List of row dictionaries if data was successfully retrieved.
              - List with a single {"error": "..."} dictionary if query failed.
    """
    sample_data = {}
    # Step 1: Build SQL query to list tables from information_schema
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

    # Worker function to fetch data for a single table
    def fetch_table_data(t):
        full_table_name = f"{t.table_catalog}.{t.table_schema}.{t.table_name}"
        data_query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

        try:
            rows = [row.asDict() for row in spark.sql(data_query).collect()]
            return (t.table_catalog, t.table_schema, t.table_name), rows
        except Exception as e:
            return (t.table_catalog, t.table_schema, t.table_name), [{"error": str(e)}]

    # Step 2: Run in parallel with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {executor.submit(fetch_table_data, t): t for t in tables}
        for future in as_completed(future_to_table):
            key, rows = future.result()
            sample_data[key] = rows

    return sample_data


# Generate table sample data for column comment AI generation
sample_data = get_sample_data(catalog, data_limit, schema, table, max_workers = default_parallelism)
print(sample_data)

# COMMAND ----------

# DBTITLE 1,Get Column Comments (Multi-Threaded)
def describe_single_column(col: Row, samples: dict, endpoint_name: str, replace_comment: bool) -> Row:
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
    key = (col.table_catalog, col.table_schema, col.table_name)
    sample_rows = samples.get(key, [])
    sample_text = str(sample_rows[:3])  # Use only first 3 rows for brevity

    # Prompt construction for AI model
    prompt = (
        f'Generate a one-sentence description of the type of information in column "{col.column_name}" '
        f'from table "{col.table_name}" in schema "{col.table_schema}" within catalog "{col.table_catalog}". '
        f'The column data type is "{col.data_type}". '
        f'Here are some sample rows from the table: {sample_text}. '
        f'This will be used as a column description, so avoid mentioning schema or catalog.'
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
        replace_comment= replace_comment,
        new_comment=new_comment
    )


def get_column_comments(catalog: str, limit: int, schema: str = None, table: str = None, max_workers: int = 8, replace_comment = False) -> DataFrame:
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
    samples = get_sample_data(catalog, limit, schema, table)


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
        futures = [executor.submit(describe_single_column, col, samples, endpoint_name, replace_comment) for col in columns]
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
commented_columns = get_column_comments(catalog, data_limit, schema, table, max_workers = default_parallelism, replace_comment = always_update)
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