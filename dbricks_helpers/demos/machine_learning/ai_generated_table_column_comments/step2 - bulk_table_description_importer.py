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
output_path = dbutils.widgets.get("Input Path") + "/bulk_comments/tables"
print(f"output_path: {output_path}")

output_file_format = "json" # or csv
print(f"output_file_format: {output_file_format}")

# Calculate default parallelism
try: default_parallelism = spark.sparkContext.defaultParallelism / 2 # Does not work with serverless
except: default_parallelism = (4 * os.cpu_count()) - 1
print(f"Number of executors: {default_parallelism}")

# COMMAND ----------

# DBTITLE 1,Read Locally Written AI Generated Table Descriptions
table_descriptions = (
    spark.read
    .format(output_file_format)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(output_path + f"/{output_file_format}")
).filter(F.col("replace_comment") == True) # This is set in the step 1 generator notebook
display(table_descriptions)

# COMMAND ----------

# DBTITLE 1,Prepare for Unity Catalog Table Description Updates
def prepare_table_descriptions_for_uc_updates(table_descriptions: DataFrame) -> DataFrame:
    """
    Prepare a DataFrame of table-level comments for Unity Catalog updates.

    Args:
        table_descriptions (DataFrame): Input DataFrame containing:
            - existing_comment (str)
            - new_comment (str)
            - replace_comment (bool)
            - table_catalog (str)
            - table_name (str)
            - table_schema (str)

    Returns:
        DataFrame: Original columns plus:
            - full_table_name (catalog.schema.table)
            - cleaned_comment (new_comment with single quotes removed)
    """
    df = table_descriptions
    df_out = (
        df.withColumn(
            "full_table_name",
            F.concat(
                F.col("table_catalog"), F.lit('.'),
                F.col("table_schema"), F.lit('.'),
                F.col("table_name")
            )
        )
        .withColumn("cleaned_comment", F.regexp_replace("new_comment", "'", ""))
    )
    return df_out


# Prepare table description updates for UC catalog tables
cleaned_table_descriptions = prepare_table_descriptions_for_uc_updates(table_descriptions)
display(cleaned_table_descriptions)

# COMMAND ----------

# DBTITLE 1,Update UC Table Descriptions
# IMPORTANT: Applying a description to a table triggers an ALTER SQL command,
# which creates a new Delta log entry (table version). This can cause
# conflicts with concurrent jobs or pipelines that modify the same table.

def apply_table_description(full_table_name: str, cleaned_comment: str) -> bool:
    """
    Apply a COMMENT ON TABLE statement for a single table.
    Each COMMENT ON TABLE is executed as an ALTER TABLE operation,
    which writes a new metadata version for the Delta table.
    Args:
        full_table_name (str): Fully-qualified table name (catalog.schema.table).
        cleaned_comment (str): Sanitized comment string (single quotes removed).
    Returns:
        bool: True if the comment was successfully applied, False if an error occurred.
    """
    query = f"COMMENT ON TABLE {full_table_name} IS '{cleaned_comment}'"
    try:
        spark.sql(query)
        return True
    except Exception as e:
        print(f"Error executing query: {query}")
        print(f"Error: {e}")
        return False


def apply_table_description_multithreaded(commented_tables, max_workers: int = 10):
    """
    Update table comments in parallel across multiple tables.
    Each COMMENT ON TABLE statement is executed as a Delta ALTER TABLE
    operation, which writes a new metadata version for the table.
    Running updates in parallel improves throughput, but each table
    must only be updated by one thread at a time to avoid metadata conflicts.
    Args:
        commented_tables (DataFrame): DataFrame containing table metadata and new comments.
            Must include:
              - full_table_name (str)
              - cleaned_comment (str)
              - replace_comment (bool)
        always_update (bool, optional): If False, only update rows where replace_comment is True.
                                        If True, update all rows. Default = False.
        max_workers (int, optional): Maximum number of threads to process tables in parallel. Default = 8.
    Returns: None
    """
    rows = commented_tables.collect()  # brings rows to driver
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {
            executor.submit(apply_table_description, row["full_table_name"], row["cleaned_comment"]): row["full_table_name"]
            for row in rows
        }
        
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                success = future.result()
                results[table] = success
                if success:
                    print(f"UPDATED description for table '{table}'")
                else:
                    print(f"FAILED to update description for table '{table}'")
            except Exception as e:
                print(f"EXCEPTION for table '{table}': {e}")


# Update all tables, parallelizing across them.  We multi-thread each table and then update the table descriptions in each concurrent thread.
apply_table_description_multithreaded(cleaned_table_descriptions, max_workers = default_parallelism)