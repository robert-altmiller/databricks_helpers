# Databricks notebook source
# MAGIC %md
# MAGIC # Traffic Data Enriched - Silver to Gold (Detailed)
# MAGIC 
# MAGIC Creates detailed gold layer with same granularity as silver
# MAGIC Primary Key: customer_id + latitude + longitude

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("silver_database", "", "Silver Database")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target Catalog")
dbutils.widgets.text("target_database", "", "Gold Database")
dbutils.widgets.text("target_table", "", "Target Table")
dbutils.widgets.text("metadata_database", "", "Metadata Database")
dbutils.widgets.text("mode", "overwrite", "Write Mode")

catalog = dbutils.widgets.get("catalog")
silver_database = dbutils.widgets.get("silver_database")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_database = dbutils.widgets.get("target_database")
target_table = dbutils.widgets.get("target_table")
metadata_database = dbutils.widgets.get("metadata_database")
mode = dbutils.widgets.get("mode")

traffic_silver_table = f"{catalog}.{silver_database}.{source_table}"
target_full_name = f"{target_catalog}.{target_database}.{target_table}"
checkpoint_table = f"{target_catalog}.{metadata_database}.checkpoints"

print(f"Source Table: {traffic_silver_table}")
print(f"Target: {target_full_name}")
print(f"Mode: {mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window

silver_df = spark.table(traffic_silver_table)
record_count = silver_df.count()

print(f"Silver records: {record_count:,}")
print("\nSilver Schema:")
silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Enriched Gold Layer

# COMMAND ----------

# Add enrichment and rankings (NO Kafka metadata in Gold)
enriched_df = silver_df.select(
    # Primary Key Components
    col("customer_id"),
    col("latitude"),
    col("longitude"),
    col("location_key"),
    
    # Business Attributes
    col("Brand"),
    col("Market"),
    col("address"),
    col("email"),
    col("email_valid"),
    
    # Traffic Info
    col("traffic_count"),
    col("event_date"),
    col("event_date_str"),
    col("event_month"),
    col("hour_of_day"),
    col("day_of_week"),
    col("day_name"),
    
    # Quality Metrics
    col("data_quality_score"),
    col("brand_valid"),
    col("market_valid"),
    col("location_valid")
).withColumn(
    # Traffic size categorization
    "traffic_category",
    when(col("traffic_count") < 100, "LOW")
    .when(col("traffic_count") < 300, "MEDIUM")
    .when(col("traffic_count") < 500, "HIGH")
    .otherwise("VERY_HIGH")
).withColumn(
    # Time of day category
    "time_of_day_category",
    when(col("hour_of_day").between(6, 11), "MORNING")
    .when(col("hour_of_day").between(12, 17), "AFTERNOON")
    .when(col("hour_of_day").between(18, 21), "EVENING")
    .otherwise("NIGHT")
).withColumn(
    # Weekend flag
    "is_weekend",
    when(col("day_of_week").isin(1, 7), True).otherwise(False)
).withColumn(
    # Add gold processing timestamp
    "gold_processed_at",
    F.current_timestamp()
)

print(f"\nEnriched gold records: {record_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# Get summary without collecting large datasets
print("\nGold Enriched Data Summary:")
print(f"  Total Records: {record_count:,}")
print(f"  Primary Key: customer_id + latitude + longitude")

# Use SQL for efficient aggregation
summary_sql = f"""
SELECT 
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT location_key) as unique_locations,
    COUNT(DISTINCT Brand) as unique_brands,
    COUNT(DISTINCT Market) as unique_markets,
    SUM(traffic_count) as total_traffic,
    AVG(data_quality_score) as avg_quality_score,
    MIN(event_date_str) as earliest_date,
    MAX(event_date_str) as latest_date
FROM ({enriched_df.createOrReplaceTempView('temp_enriched')})
SELECT * FROM temp_enriched
"""

# Create temp view and query
enriched_df.createOrReplaceTempView("temp_enriched_gold")
summary = spark.sql("""
SELECT 
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT location_key) as unique_locations,
    COUNT(DISTINCT Brand) as unique_brands,
    COUNT(DISTINCT Market) as unique_markets,
    SUM(traffic_count) as total_traffic,
    AVG(data_quality_score) as avg_quality_score,
    MIN(event_date_str) as earliest_date,
    MAX(event_date_str) as latest_date
FROM temp_enriched_gold
""").first()

print(f"  Unique Customers: {summary['unique_customers']:,}")
print(f"  Unique Locations: {summary['unique_locations']:,}")
print(f"  Unique Brands: {summary['unique_brands']}")
print(f"  Unique Markets: {summary['unique_markets']}")
print(f"  Total Traffic: {summary['total_traffic']:,}")
print(f"  Avg Quality Score: {summary['avg_quality_score']:.2f}")
print(f"  Date Range: {summary['earliest_date']} to {summary['latest_date']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

enriched_df.write \
    .mode(mode) \
    .option("overwriteSchema", "true") \
    .partitionBy("Brand", "Market", "event_date_str") \
    .saveAsTable(target_full_name)

print(f"✅ Wrote {record_count:,} records to {target_full_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Table

# COMMAND ----------

# Optimize and Z-order by primary key
print(f"\nOptimizing table {target_full_name}...")
spark.sql(f"OPTIMIZE {target_full_name} ZORDER BY (customer_id, latitude, longitude)")
print(f"✅ Table optimized with Z-ordering on primary key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Customer Location Dimension Table

# COMMAND ----------

# Create a dimension table with unique customer locations (customer_id + lat + lon + address)
print("\n" + "="*80)
print("CREATING CUSTOMER LOCATION DIMENSION TABLE")
print("="*80)

customer_location_table = f"{target_catalog}.{target_database}.customer_locations"

# Get distinct customer locations
customer_location_df = silver_df.select(
    col("customer_id"),
    col("latitude"),
    col("longitude"),
    col("address")
).distinct()

location_count = customer_location_df.count()
print(f"\nUnique customer locations: {location_count:,}")

# Write to customer location dimension table
customer_location_df.write \
    .mode(mode) \
    .option("overwriteSchema", "true") \
    .saveAsTable(customer_location_table)

print(f"✅ Wrote {location_count:,} unique locations to {customer_location_table}")

# Optimize customer location table
print(f"\nOptimizing table {customer_location_table}...")
spark.sql(f"OPTIMIZE {customer_location_table} ZORDER BY (customer_id, latitude, longitude)")
print(f"✅ Table optimized with Z-ordering on primary key")

print("="*80)

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
        'silver_to_gold',
        current_timestamp(),
        current_timestamp(),
        {record_count}
    )
""")

# Add checkpoint for customer location table
spark.sql(f"""
    INSERT INTO {checkpoint_table}
    VALUES (
        'customer_locations',
        'silver_to_gold',
        current_timestamp(),
        current_timestamp(),
        {location_count}
    )
""")

print(f"✅ Checkpoint updated")
print(f"✅ Processing complete:")
print(f"   - traffic_data_enriched: {record_count:,} records")
print(f"   - customer_locations: {location_count:,} unique locations")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

