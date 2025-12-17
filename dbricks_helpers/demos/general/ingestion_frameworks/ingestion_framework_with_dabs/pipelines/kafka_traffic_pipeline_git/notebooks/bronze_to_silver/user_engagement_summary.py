# Databricks notebook source
# MAGIC %md
# MAGIC # User Engagement Summary - Bronze to Silver
# MAGIC 
# MAGIC Creates user-level aggregations and engagement metrics from Kafka traffic data

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
from pyspark.sql.functions import col, trim, lower, when, coalesce, lit, collect_set, concat_ws

bronze_df = spark.table(source_full_name)
record_count = bronze_df.count()
print(f"Bronze records: {record_count:,}")

# Show sample data
print("\nSample Bronze Data:")
bronze_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Email and Prepare Data

# COMMAND ----------

# Clean and validate data first
cleaned_df = bronze_df.select(
    col("customer_id"),
    col("latitude"),
    col("longitude"),
    col("address"),
    lower(trim(col("email"))).alias("email"),
    col("Brand"),
    col("Market"),
    col("event_date"),
    col("traffic_count"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("kafka_timestamp"),
    col("ingestion_timestamp")
).filter(
    # Filter valid records with required fields
    col("customer_id").isNotNull() &
    col("latitude").isNotNull() &
    col("longitude").isNotNull() &
    col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$") &
    col("Brand").isNotNull() &
    col("Market").isNotNull() &
    (col("traffic_count") > 0)
)

cleaned_count = cleaned_df.count()
print(f"Cleaned records with valid emails: {cleaned_count:,}")
print(f"Filtered out: {record_count - cleaned_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create User Engagement Aggregations

# COMMAND ----------

user_engagement_df = cleaned_df.groupBy("customer_id").agg(
    # Customer identifiers
    F.first("email").alias("email"),
    
    # Location metrics
    F.countDistinct(F.concat_ws("_", col("latitude"), col("longitude"))).alias("unique_locations"),
    F.collect_set(col("address")).alias("addresses_visited"),
    concat_ws("; ", F.collect_set(col("address"))).alias("addresses_visited_str"),
    F.avg("latitude").alias("avg_latitude"),
    F.avg("longitude").alias("avg_longitude"),
    
    # Basic counts
    F.count("customer_id").alias("total_events"),
    F.sum("traffic_count").alias("total_traffic"),
    F.avg("traffic_count").alias("avg_traffic_per_event"),
    
    # Brand interaction
    F.countDistinct("Brand").alias("brands_engaged"),
    F.collect_set("Brand").alias("brand_list"),
    concat_ws(", ", F.collect_set("Brand")).alias("brands_engaged_str"),
    
    # Market interaction
    F.countDistinct("Market").alias("markets_engaged"),
    F.collect_set("Market").alias("market_list"),
    concat_ws(", ", F.collect_set("Market")).alias("markets_engaged_str"),
    
    # Most active brand (brand with most events)
    F.first("Brand").alias("primary_brand"),
    
    # Most active market (market with most events)
    F.first("Market").alias("primary_market"),
    
    # Time-based metrics
    F.min("event_date").alias("first_seen_date"),
    F.max("event_date").alias("last_seen_date"),
    F.countDistinct(F.date_format("event_date", "yyyy-MM-dd")).alias("active_days"),
    
    # Kafka metadata
    F.min("kafka_timestamp").alias("first_kafka_timestamp"),
    F.max("kafka_timestamp").alias("last_kafka_timestamp"),
    F.countDistinct("topic").alias("kafka_topics_count")
)

# Add derived metrics
user_engagement_final = user_engagement_df.withColumn(
    # Calculate engagement duration in days
    "engagement_duration_days",
    F.datediff(col("last_seen_date"), col("first_seen_date"))
).withColumn(
    # User engagement score (0-100)
    # Based on: total events (30%), brands engaged (25%), markets engaged (20%), unique locations (15%), active days (10%)
    "engagement_score",
    F.least(
        lit(100),
        F.round(
            (F.least(col("total_events"), lit(50)) / 50 * 30) +
            (F.least(col("brands_engaged"), lit(5)) / 5 * 25) +
            (F.least(col("markets_engaged"), lit(5)) / 5 * 20) +
            (F.least(col("unique_locations"), lit(10)) / 10 * 15) +
            (F.least(col("active_days"), lit(30)) / 30 * 10),
            2
        )
    )
).withColumn(
    # User segment based on engagement score
    "user_segment",
    when(col("engagement_score") >= 80, "HIGH_ENGAGED")
    .when(col("engagement_score") >= 50, "MEDIUM_ENGAGED")
    .when(col("engagement_score") >= 20, "LOW_ENGAGED")
    .otherwise("MINIMAL_ENGAGED")
).withColumn(
    # User type based on brand interaction
    "user_type",
    when(col("brands_engaged") >= 3, "MULTI_BRAND")
    .when(col("brands_engaged") == 2, "DUAL_BRAND")
    .otherwise("SINGLE_BRAND")
).withColumn(
    # Market penetration flag
    "multi_market_user",
    when(col("markets_engaged") > 1, True).otherwise(False)
).withColumn(
    # Activity frequency (events per day)
    "avg_events_per_day",
    F.round(
        col("total_events") / F.greatest(col("engagement_duration_days"), lit(1)),
        2
    )
).withColumn(
    # User lifecycle status
    "lifecycle_status",
    when(F.datediff(F.current_date(), col("last_seen_date")) <= 7, "ACTIVE")
    .when(F.datediff(F.current_date(), col("last_seen_date")) <= 30, "RECENT")
    .when(F.datediff(F.current_date(), col("last_seen_date")) <= 90, "DORMANT")
    .otherwise("INACTIVE")
)

# Reorder columns for better readability
user_engagement_final = user_engagement_final.select(
    col("customer_id"),
    col("email"),
    col("user_segment"),
    col("user_type"),
    col("engagement_score"),
    col("lifecycle_status"),
    col("unique_locations"),
    col("addresses_visited_str"),
    col("avg_latitude"),
    col("avg_longitude"),
    col("total_events"),
    col("total_traffic"),
    col("avg_traffic_per_event"),
    col("brands_engaged"),
    col("brands_engaged_str"),
    col("primary_brand"),
    col("markets_engaged"),
    col("markets_engaged_str"),
    col("primary_market"),
    col("multi_market_user"),
    col("first_seen_date"),
    col("last_seen_date"),
    col("engagement_duration_days"),
    col("active_days"),
    col("avg_events_per_day"),
    col("first_kafka_timestamp"),
    col("last_kafka_timestamp"),
    col("kafka_topics_count"),
    F.current_timestamp().alias("processed_at")
)

customer_count = user_engagement_final.count()
print(f"\nCustomer engagement records created: {customer_count:,}")

# Show sample data
print("\nSample Customer Engagement Data:")
user_engagement_final.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Engagement Analytics Summary

# COMMAND ----------

# Segment distribution
segment_summary = user_engagement_final.groupBy("user_segment").agg(
    F.count("customer_id").alias("customer_count"),
    F.avg("engagement_score").alias("avg_engagement_score"),
    F.sum("total_events").alias("total_events"),
    F.sum("total_traffic").alias("total_traffic")
).orderBy(F.desc("customer_count"))

print("Customer Segment Distribution:")
segment_summary.show(truncate=False)

# User type distribution
type_summary = user_engagement_final.groupBy("user_type").agg(
    F.count("customer_id").alias("customer_count"),
    F.avg("engagement_score").alias("avg_engagement_score")
).orderBy(F.desc("customer_count"))

print("\nCustomer Type Distribution:")
type_summary.show(truncate=False)

# Lifecycle status
lifecycle_summary = user_engagement_final.groupBy("lifecycle_status").agg(
    F.count("customer_id").alias("customer_count"),
    F.avg("engagement_score").alias("avg_engagement_score")
).orderBy(F.desc("customer_count"))

print("\nCustomer Lifecycle Status:")
lifecycle_summary.show(truncate=False)

# Top brands by unique customers
brand_customers = user_engagement_final.groupBy("primary_brand").agg(
    F.count("customer_id").alias("customer_count")
).orderBy(F.desc("customer_count"))

print("\nTop Brands by Customer Count:")
brand_customers.show(truncate=False)

# Overall metrics
overall_metrics = user_engagement_final.agg(
    F.count("customer_id").alias("total_unique_customers"),
    F.avg("engagement_score").alias("avg_engagement_score"),
    F.sum("total_events").alias("total_events"),
    F.sum("total_traffic").alias("total_traffic"),
    F.avg("brands_engaged").alias("avg_brands_per_customer"),
    F.avg("markets_engaged").alias("avg_markets_per_customer"),
    F.avg("unique_locations").alias("avg_locations_per_customer")
)

print("\nOverall Customer Metrics:")
overall_metrics.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Target

# COMMAND ----------

user_engagement_final.write \
    .mode(mode) \
    .option("overwriteSchema", "true") \
    .partitionBy("user_segment") \
    .saveAsTable(target_full_name)

print(f"✅ Wrote {customer_count:,} customer records to {target_full_name}")

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
        {customer_count}
    )
""")

print(f"✅ Checkpoint updated")
print(f"✅ Processing complete: {customer_count:,} customers processed")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")


