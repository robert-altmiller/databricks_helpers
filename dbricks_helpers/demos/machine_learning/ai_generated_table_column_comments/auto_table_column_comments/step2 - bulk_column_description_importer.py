# Databricks notebook source
# DBTITLE 1,Library Imports
import os
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Setup Databricks Widgets
dbutils.widgets.text("Input Path", "", "Enter Base Volumes Path (Mandatory):")

# COMMAND ----------

# DBTITLE 1,Local Parameters
output_path = dbutils.widgets.get("Input Path") + "/bulk_comments/columns"
print(f"output_path: {output_path}")

output_file_format = "json" # or csv
print(f"output_file_format: {output_file_format}")

# Calculate default parallelism
try: default_parallelism = spark.sparkContext.defaultParallelism / 2 # Does not work with serverless
except: default_parallelism = (4 * os.cpu_count()) - 1
print(f"Number of executors: {default_parallelism}")

# COMMAND ----------

# DBTITLE 1,Read Locally Written AI Generated Column Comments
commented_columns = (
    spark.read
    .format(output_file_format)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(output_path + f"/{output_file_format}")
).filter(F.col("replace_comment") == True) # This is set in the step 1 generator notebook
display(commented_columns)

# COMMAND ----------

# DBTITLE 1,Prepare for Unity Catalog Table Column Updates
def prepare_comments_for_uc_updates(commented_columns: DataFrame) -> DataFrame:
    """
    Prepare a DataFrame of column comments for Unity Catalog updates.
    Args:
        commented_columns (DataFrame): Input DataFrame containing at least:
            - table_catalog (str)
            - table_schema (str)
            - table_name (str)
            - column_name (str)
            - new_comment (str)
            - replace_comment (bool)
    Returns:
        DataFrame: A DataFrame with:
            - full_table_name (catalog.schema.table)
            - column_name
            - new_comment
            - cleaned_comment (new_comment with single quotes removed)
    """
    df = commented_columns
    df_out = (
        df.select(
            F.concat(F.col("table_catalog"), F.lit('.'), F.col("table_schema"), F.lit('.'), F.col("table_name"))
                .alias("full_table_name"),
            "column_name",
            "new_comment"
        )
        .withColumn("cleaned_comment", F.regexp_replace("new_comment", "'", ""))
    )
    return df_out


# Prepare column updates for UC catalog tables
# cleaned_comments = prepare_comments_for_uc_updates(commented_columns)
# display(cleaned_comments)

# COMMAND ----------

# DBTITLE 1,Update UC Table Column Comments
# IMPORTANT: Applying a column comment to a table triggers an ALTER SQL command,
# which creates a new Delta log entry (table version). This can cause
# conflicts with concurrent jobs or pipelines that modify the same table.

def update_table_column_comments_iterative(table, cols) -> None:
    """
    Sequentially update all column comments for a single table.
    Each COMMENT ON COLUMN statement is executed as a Delta ALTER TABLE
    operation, which writes a new metadata version for the table.
    Running updates sequentially per table avoids Delta concurrency
    conflicts (MetadataChangedException).
    Args:
        table (str): Fully-qualified table name (catalog.schema.table).
        cols (list[Row]): List of Row objects containing:
            - column_name (str)
            - cleaned_comment (str)
    Returns: None
    """
    for r in cols:
        query = f"COMMENT ON COLUMN {table}.{r['column_name']} IS '{r['cleaned_comment']}'"
        try:
            spark.sql(query)
        except Exception as e:
            print(f"FAILED update on {table}.{r['column_name']}: {e}")


def update_table_column_comments_threaded(commented_columns: DataFrame, max_workers: int = 8) -> None:
    """
    Update column comments across multiple tables in parallel.
    Groups updates by table so each table is only touched by one thread.
    Within a table, updates are applied sequentially (safe for Delta metadata).
    Across tables, multiple threads run in parallel for faster execution.
    Args:
        commented_columns (DataFrame): DataFrame with column metadata and new comments.
            Must include:
              - full_table_name (str)
              - column_name (str)
              - cleaned_comment (str)
        max_workers (int, optional): Maximum number of tables to process in parallel. Default = 8.
    Returns: None
    """
    # Step 1: Prepare data
    prepared = prepare_comments_for_uc_updates(commented_columns)
    rows = prepared.collect()

    # Step 2: Group rows by table
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["full_table_name"]].append(r)

    # Step 3: Assign one thread per table
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_table_column_comments_iterative, tbl, cols): tbl for tbl, cols in grouped.items()}
        for f in as_completed(futures):
            table = futures[f]
            try:
                f.result()
                print(f"FINISHED updates for table: {table}")
            except Exception as e:
                print(f"FAILED updates for table {table}: {e}")



# Update all tables, parallelizing across them.  We multi-thread each table and then update iteratively.
# This is thread safe because each table's updates are sequential
update_table_column_comments_threaded(commented_columns, max_workers=default_parallelism)