# Databricks notebook source
"""
Example Notebook for Loyalty 2.0 Pipeline
This is a template notebook - customize it for your use case
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Example Pipeline Notebook
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC - Reading from source
# MAGIC - Transforming data
# MAGIC - Writing to target

# COMMAND ----------

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime

# COMMAND ----------

# Get parameters (passed from databricks.yml)
dbutils.widgets.text("catalog", "loyalty_dev", "Catalog Name")
dbutils.widgets.text("source_schema", "bronze", "Source Schema")
dbutils.widgets.text("target_schema", "silver", "Target Schema")
dbutils.widgets.text("table_name", "example_table", "Table Name")

catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
target_schema = dbutils.widgets.get("target_schema")
table_name = dbutils.widgets.get("table_name")

print(f"Catalog: {catalog}")
print(f"Source Schema: {source_schema}")
print(f"Target Schema: {target_schema}")
print(f"Table: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Source Data

# COMMAND ----------

# Example: Read from bronze layer
source_table = f"{catalog}.{source_schema}.{table_name}"

try:
    df = spark.table(source_table)
    print(f"✓ Read {df.count()} rows from {source_table}")
    df.display()
except Exception as e:
    print(f"⚠ Table not found, creating sample data")
    # Create sample data if table doesn't exist
    df = spark.createDataFrame([
        (1, "Customer A", "2024-01-01", 100.0),
        (2, "Customer B", "2024-01-02", 150.0),
        (3, "Customer C", "2024-01-03", 200.0),
    ], ["id", "customer_name", "transaction_date", "amount"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform Data

# COMMAND ----------

# Example transformations
df_transformed = df \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("pipeline_name", lit("template_pipeline")) \
    .withColumn("year", col("transaction_date").substr(1, 4))

print(f"✓ Transformed {df_transformed.count()} rows")
df_transformed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Checks

# COMMAND ----------

# Example: Check for nulls
null_check = df_transformed.filter(col("customer_name").isNull()).count()
if null_check > 0:
    print(f"⚠ Warning: Found {null_check} rows with null customer_name")
else:
    print("✓ No null values in customer_name")

# Example: Check for duplicates
duplicate_check = df_transformed.groupBy("id").count().filter(col("count") > 1).count()
if duplicate_check > 0:
    print(f"⚠ Warning: Found {duplicate_check} duplicate IDs")
else:
    print("✓ No duplicate IDs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Target

# COMMAND ----------

# Write to target table
target_table = f"{catalog}.{target_schema}.{table_name}"

df_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

# Print summary
print("=" * 80)
print("Pipeline Execution Summary")
print("=" * 80)
print(f"Source: {source_table}")
print(f"Target: {target_table}")
print(f"Rows Processed: {df_transformed.count()}")
print(f"Timestamp: {datetime.now()}")
print("=" * 80)
print("✅ Pipeline completed successfully!")

# COMMAND ----------


