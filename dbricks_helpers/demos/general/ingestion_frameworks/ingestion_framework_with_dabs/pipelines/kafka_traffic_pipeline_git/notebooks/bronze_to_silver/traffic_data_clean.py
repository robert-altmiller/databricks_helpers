# Databricks notebook source
# MAGIC %md
# MAGIC # Traffic Data Clean - Bronze to Silver
# MAGIC 
# MAGIC Cleans and transforms Kafka traffic data from bronze to silver layer

# COMMAND ----------

# Get parameters
dbutils.widgets.text("source_catalog", "", "Source Catalog")
dbutils.widgets.text("source_database", "", "Source Database")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target Catalog")
dbutils.widgets.text("target_database", "", "Target Database")
dbutils.widgets.text("target_table", "", "Target Table")
dbutils.widgets.text("metadata_database", "framework_metadata", "Metadata Database")
dbutils.widgets.text("mode", "overwrite", "Write Mode")

source_catalog = dbutils.widgets.get("source_catalog")
source_database = dbutils.widgets.get("source_database")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_database = dbutils.widgets.get("target_database")
target_table = dbutils.widgets.get("target_table")
metadata_database = dbutils.widgets.get("metadata_database")
mode = dbutils.widgets.get("mode")

source_full_name = f"{source_catalog}.{source_database}.{source_table}"
target_full_name = f"{target_catalog}.{target_database}.{target_table}"
checkpoint_table = f"{target_catalog}.{metadata_database}.checkpoints"

print(f"Source: {source_full_name}")
print(f"Target: {target_full_name}")
print(f"Mode: {mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, lower, when, coalesce, lit

bronze_df = spark.table(source_full_name)
record_count = bronze_df.count()
print(f"Bronze records: {record_count:,}")

# Show sample data
print("\nSample Bronze Data:")
bronze_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean and Transform

# COMMAND ----------

silver_df = bronze_df.select(
    # Primary key components: customer_id + latitude + longitude
    col("customer_id"),
    col("latitude"),
    col("longitude"),
    
    # Create composite primary key
    F.concat_ws("_", col("customer_id"), col("latitude"), col("longitude")).alias("location_key"),
    
    # Brand and Market
    col("Brand"),
    col("event_date"),
    col("Market"),
    
    # Location details
    col("address"),
    
    # Clean and validate email
    lower(trim(col("email"))).alias("email"),
    when(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"), True)
        .otherwise(False).alias("email_valid"),
    
    col("traffic_count"),
    
    # Extract time dimensions
    F.hour(col("event_date")).alias("hour_of_day"),
    F.dayofweek(col("event_date")).alias("day_of_week"),
    F.date_format(col("event_date"), "yyyy-MM-dd").alias("event_date_str"),
    F.date_format(col("event_date"), "yyyy-MM").alias("event_month"),
    F.date_format(col("event_date"), "EEEE").alias("day_name"),
    
    # Brand and Market validation
    when(col("Brand").isNotNull() & (col("Brand") != ""), True)
        .otherwise(False).alias("brand_valid"),
    when(col("Market").isNotNull() & (col("Market") != ""), True)
        .otherwise(False).alias("market_valid"),
    
    # Location validation
    when(col("latitude").isNotNull() & col("longitude").isNotNull(), True)
        .otherwise(False).alias("location_valid"),
    
    # Kafka metadata
    col("topic"),
    col("partition"),
    col("offset"),
    col("kafka_timestamp"),
    col("ingestion_timestamp"),
    
    # Calculate processing latency (time between kafka timestamp and ingestion)
    F.round(
        (F.unix_timestamp(col("ingestion_timestamp")) - F.unix_timestamp(col("kafka_timestamp"))) / 60, 2
    ).alias("processing_latency_minutes"),
    
    # Data quality score (0-100)
    (
        when(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"), 25).otherwise(0) +
        when(col("traffic_count") > 0, 25).otherwise(0) +
        when(col("Brand").isNotNull() & (col("Brand") != ""), 15).otherwise(0) +
        when(col("Market").isNotNull() & (col("Market") != ""), 15).otherwise(0) +
        when(col("latitude").isNotNull() & col("longitude").isNotNull(), 20).otherwise(0)
    ).alias("data_quality_score"),
    
    # Metadata
    F.current_timestamp().alias("processed_at")
)

# Filter out invalid records
silver_df_filtered = silver_df.filter(
    (col("traffic_count") > 0) &      # Must have valid traffic count
    (col("Brand").isNotNull()) &       # Must have brand
    (col("Market").isNotNull()) &      # Must have market
    (col("customer_id").isNotNull()) & # Must have customer_id
    (col("latitude").isNotNull()) &    # Must have latitude
    (col("longitude").isNotNull())     # Must have longitude
)

cleaned_count = silver_df_filtered.count()
filtered_out = record_count - cleaned_count

print(f"\nCleaned records: {cleaned_count:,}")
print(f"Filtered out (invalid): {filtered_out:,}")
if record_count > 0:
    print(f"Data quality pass rate: {(cleaned_count/record_count*100):.2f}%")

# Show sample cleaned data
print("\nSample Cleaned Data:")
silver_df_filtered.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# Quality metrics
quality_summary = silver_df_filtered.agg(
    F.count("*").alias("total_records"),
    F.sum(when(col("email_valid"), 1).otherwise(0)).alias("valid_emails"),
    F.sum(when(col("brand_valid"), 1).otherwise(0)).alias("valid_brands"),
    F.sum(when(col("market_valid"), 1).otherwise(0)).alias("valid_markets"),
    F.avg("data_quality_score").alias("avg_quality_score"),
    F.min("event_date").alias("earliest_event"),
    F.max("event_date").alias("latest_event"),
    F.sum("traffic_count").alias("total_traffic")
)

print("Data Quality Summary:")
quality_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Target

# COMMAND ----------

silver_df_filtered.write \
    .mode(mode) \
    .option("overwriteSchema", "true") \
    .partitionBy("Brand", "Market") \
    .saveAsTable(target_full_name)

print(f"✅ Wrote {cleaned_count:,} records to {target_full_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Checkpoint

# COMMAND ----------

# Ensure checkpoint table exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {checkpoint_table} (
    table_name STRING,
    layer STRING,
    last_watermark TIMESTAMP,
    last_run_timestamp TIMESTAMP,
    records_processed BIGINT
)
""")

spark.sql(f"""
    INSERT INTO {checkpoint_table}
    VALUES (
        '{target_table}',
        'bronze_to_silver',
        current_timestamp(),
        current_timestamp(),
        {cleaned_count}
    )
""")

print(f"✅ Checkpoint updated")
print(f"✅ Processing complete: {cleaned_count:,} records")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

