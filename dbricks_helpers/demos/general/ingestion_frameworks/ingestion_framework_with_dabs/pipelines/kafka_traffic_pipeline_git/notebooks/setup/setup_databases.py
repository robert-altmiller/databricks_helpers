# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Databases and Verify Tables
# MAGIC 
# MAGIC Verifies Kafka bronze table exists and creates Silver/Gold databases if needed

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("bronze_database", "", "Bronze Database")
dbutils.widgets.text("silver_database", "", "Silver Database")
dbutils.widgets.text("gold_database", "", "Gold Database")
dbutils.widgets.text("metadata_database", "", "Metadata Database")
dbutils.widgets.dropdown("force_recreate", "false", ["true", "false"], "Force Recreate Tables")

catalog = dbutils.widgets.get("catalog")
bronze_database = dbutils.widgets.get("bronze_database")
silver_database = dbutils.widgets.get("silver_database")
gold_database = dbutils.widgets.get("gold_database")
metadata_database = dbutils.widgets.get("metadata_database")
force_recreate = dbutils.widgets.get("force_recreate").lower() == "true"

print(f"Catalog: {catalog}")
print(f"Bronze DB: {bronze_database}")
print(f"Silver DB: {silver_database}")
print(f"Gold DB: {gold_database}")
print(f"Metadata DB: {metadata_database}")
print(f"Force Recreate: {force_recreate}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Bronze Table Exists

# COMMAND ----------

# Check if Kafka bronze table exists
def check_table_exists(catalog, database, table):
    """Check if a table exists"""
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{database}.{table}")
        return True
    except Exception:
        return False

kafka_bronze_exists = check_table_exists(catalog, bronze_database, "kafka_data_bronze")

print("\n" + "="*80)
print("KAFKA BRONZE TABLE CHECK")
print("="*80)
print(f"kafka_data_bronze: {'✅ EXISTS' if kafka_bronze_exists else '❌ NOT FOUND'}")
print("="*80)

if not kafka_bronze_exists:
    print("\n⚠️  WARNING: Kafka bronze table not found!")
    print(f"   Expected table: {catalog}.{bronze_database}.kafka_data_bronze")
    print(f"   This table will be created when the Kafka ingestion pipeline runs.")
    print(f"\n   ✅ Continuing with database setup...")
    print(f"   The bronze table will be created automatically by the Kafka ingestion job.")
else:
    print(f"\n✅ Kafka bronze table exists")
    # Show table info
    record_count = spark.table(f"{catalog}.{bronze_database}.kafka_data_bronze").count()
    print(f"   Records in table: {record_count:,}")
    
    # Show schema
    print("\n   Current Schema:")
    spark.sql(f"DESCRIBE TABLE {catalog}.{bronze_database}.kafka_data_bronze").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Old Tables (if force_recreate=true)

# COMMAND ----------

if force_recreate:
    print("\n" + "="*80)
    print("FORCE RECREATE MODE ENABLED - DROPPING OLD TABLES")
    print("="*80)
    print("⚠️  WARNING: This will delete all data in Silver and Gold tables!")
    print("="*80)
    
    
    # Drop Silver and Gold Tables
    print("\nDropping Silver and Gold Tables...")
    tables_to_drop = [
        # Silver tables
        f"{catalog}.{silver_database}.slv_traffic_data_cleaned",
        f"{catalog}.{silver_database}.slv_user_engagement_summary",
        f"{catalog}.{silver_database}.slv_customer_engagement_summary",
        # Gold tables (current)
        f"{catalog}.{gold_database}.gld_traffic_data_enriched",
        f"{catalog}.{gold_database}.gld_customer_locations",
        # Old gold tables (from previous versions)
        f"{catalog}.{gold_database}.gld_traffic_analytics",
        f"{catalog}.{gold_database}.gld_traffic_hourly_patterns",
        f"{catalog}.{gold_database}.gld_traffic_dow_patterns"
    ]
    
    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"  ✅ Dropped: {table}")
        except Exception as e:
            print(f"  ⚠️  Could not drop {table}: {str(e)}")
    
    print(f"\n✅ All old tables dropped successfully ({len(tables_to_drop)} tables)")
    print("="*80)
else:
    print("\n" + "="*80)
    print("FORCE RECREATE MODE: DISABLED")
    print("="*80)
    print("Existing tables will be preserved.")
    print("To drop and recreate tables, set force_recreate='true'")
    print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Databases

# COMMAND ----------

# Create all databases if they don't exist
print("\nCreating databases if they don't exist...")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{bronze_database}")
print(f"✅ Verified {catalog}.{bronze_database}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{silver_database}")
print(f"✅ Created/Verified {catalog}.{silver_database}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{gold_database}")
print(f"✅ Created/Verified {catalog}.{gold_database}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{metadata_database}")
print(f"✅ Created/Verified {catalog}.{metadata_database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Checkpoint Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{metadata_database}.checkpoints (
    table_name STRING,
    layer STRING,
    last_watermark TIMESTAMP,
    last_run_timestamp TIMESTAMP,
    records_processed BIGINT
) USING DELTA
""")

print(f"✅ Created/Verified checkpoint table: {catalog}.{metadata_database}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify/Create Silver Table Schema (Optional)

# COMMAND ----------

# Optional: Pre-create silver table with explicit schema
# This ensures the schema is correct before the transformation runs

silver_table_exists = check_table_exists(catalog, silver_database, "slv_traffic_data_cleaned")

if not silver_table_exists:
    print(f"\nCreating Silver table schema: {catalog}.{silver_database}.slv_traffic_data_cleaned")
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{silver_database}.slv_traffic_data_cleaned (
        customer_id STRING COMMENT 'Customer ID (primary key component)',
        latitude DOUBLE COMMENT 'Latitude coordinate (primary key component)',
        longitude DOUBLE COMMENT 'Longitude coordinate (primary key component)',
        location_key STRING COMMENT 'Composite key: customer_id_latitude_longitude',
        Brand STRING COMMENT 'Brand name (Athleta, GAP, etc.)',
        event_date TIMESTAMP COMMENT 'Event timestamp',
        Market STRING COMMENT 'Market region (CA, EU, US, etc.)',
        address STRING COMMENT 'Physical address',
        email STRING COMMENT 'Cleaned email address',
        email_valid BOOLEAN COMMENT 'Email validation flag',
        traffic_count INT COMMENT 'Traffic count for this event',
        hour_of_day INT COMMENT 'Hour of day (0-23)',
        day_of_week INT COMMENT 'Day of week (1-7)',
        event_date_str STRING COMMENT 'Event date as string (yyyy-MM-dd)',
        event_month STRING COMMENT 'Event month (yyyy-MM)',
        day_name STRING COMMENT 'Day name (Monday, Tuesday, etc.)',
        brand_valid BOOLEAN COMMENT 'Brand validation flag',
        market_valid BOOLEAN COMMENT 'Market validation flag',
        location_valid BOOLEAN COMMENT 'Location validation flag',
        topic STRING COMMENT 'Kafka topic name',
        partition INT COMMENT 'Kafka partition',
        offset BIGINT COMMENT 'Kafka offset',
        kafka_timestamp TIMESTAMP COMMENT 'Kafka message timestamp',
        ingestion_timestamp TIMESTAMP COMMENT 'Ingestion timestamp',
        processing_latency_minutes DOUBLE COMMENT 'Processing latency in minutes',
        data_quality_score INT COMMENT 'Data quality score (0-100)',
        processed_at TIMESTAMP COMMENT 'Silver processing timestamp'
    ) USING DELTA
    PARTITIONED BY (Brand, Market)
    COMMENT 'Cleaned traffic data from Kafka bronze layer with composite key (customer_id + latitude + longitude)'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    print(f"✅ Created Silver table: {catalog}.{silver_database}.slv_traffic_data_cleaned")
else:
    print(f"✅ Silver table already exists: {catalog}.{silver_database}.slv_traffic_data_cleaned")

# Customer engagement summary table (renamed from user_engagement_summary)
customer_engagement_exists = check_table_exists(catalog, silver_database, "slv_customer_engagement_summary")

if not customer_engagement_exists:
    print(f"\nCreating Silver table schema: {catalog}.{silver_database}.slv_customer_engagement_summary")
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{silver_database}.slv_customer_engagement_summary (
        customer_id STRING COMMENT 'Customer ID (primary key)',
        email STRING COMMENT 'Customer email address',
        user_segment STRING COMMENT 'Customer engagement segment (HIGH/MEDIUM/LOW/MINIMAL)',
        user_type STRING COMMENT 'Customer type (MULTI_BRAND/DUAL_BRAND/SINGLE_BRAND)',
        engagement_score DOUBLE COMMENT 'Customer engagement score (0-100)',
        lifecycle_status STRING COMMENT 'Customer lifecycle status (ACTIVE/RECENT/DORMANT/INACTIVE)',
        unique_locations BIGINT COMMENT 'Number of unique locations visited',
        addresses_visited_str STRING COMMENT 'Semicolon-separated list of addresses',
        avg_latitude DOUBLE COMMENT 'Average latitude of customer locations',
        avg_longitude DOUBLE COMMENT 'Average longitude of customer locations',
        total_events BIGINT COMMENT 'Total number of events',
        total_traffic BIGINT COMMENT 'Total traffic count',
        avg_traffic_per_event DOUBLE COMMENT 'Average traffic per event',
        brands_engaged BIGINT COMMENT 'Number of unique brands engaged',
        brands_engaged_str STRING COMMENT 'Comma-separated list of brands',
        primary_brand STRING COMMENT 'Most active brand for this customer',
        markets_engaged BIGINT COMMENT 'Number of unique markets engaged',
        markets_engaged_str STRING COMMENT 'Comma-separated list of markets',
        primary_market STRING COMMENT 'Most active market for this customer',
        multi_market_user BOOLEAN COMMENT 'Flag if customer engaged with multiple markets',
        first_seen_date TIMESTAMP COMMENT 'First customer activity timestamp',
        last_seen_date TIMESTAMP COMMENT 'Last customer activity timestamp',
        engagement_duration_days INT COMMENT 'Duration between first and last activity',
        active_days BIGINT COMMENT 'Number of unique active days',
        avg_events_per_day DOUBLE COMMENT 'Average events per day',
        first_kafka_timestamp TIMESTAMP COMMENT 'First Kafka message timestamp',
        last_kafka_timestamp TIMESTAMP COMMENT 'Last Kafka message timestamp',
        kafka_topics_count BIGINT COMMENT 'Number of unique Kafka topics',
        processed_at TIMESTAMP COMMENT 'Silver processing timestamp'
    ) USING DELTA
    PARTITIONED BY (user_segment)
    COMMENT 'Customer-level engagement summary with location and engagement metrics'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    print(f"✅ Created Silver table: {catalog}.{silver_database}.slv_customer_engagement_summary")
else:
    print(f"✅ Silver table already exists: {catalog}.{silver_database}.slv_customer_engagement_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify/Create Gold Table Schema (Optional)

# COMMAND ----------

# Gold enriched table (detailed - same granularity as silver, no Kafka metadata)
gold_enriched_exists = check_table_exists(catalog, gold_database, "gld_traffic_data_enriched")

if not gold_enriched_exists:
    print(f"\nCreating Gold table schema: {catalog}.{gold_database}.gld_traffic_data_enriched")
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{gold_database}.gld_traffic_data_enriched (
        customer_id STRING COMMENT 'Customer ID (primary key component)',
        latitude DOUBLE COMMENT 'Latitude coordinate (primary key component)',
        longitude DOUBLE COMMENT 'Longitude coordinate (primary key component)',
        location_key STRING COMMENT 'Composite key: customer_id_latitude_longitude',
        Brand STRING COMMENT 'Brand name',
        Market STRING COMMENT 'Market region',
        address STRING COMMENT 'Physical address',
        email STRING COMMENT 'Customer email',
        email_valid BOOLEAN COMMENT 'Email validation flag',
        traffic_count INT COMMENT 'Traffic count',
        event_date TIMESTAMP COMMENT 'Event timestamp',
        event_date_str STRING COMMENT 'Event date (yyyy-MM-dd)',
        event_month STRING COMMENT 'Event month (yyyy-MM)',
        hour_of_day INT COMMENT 'Hour of day (0-23)',
        day_of_week INT COMMENT 'Day of week (1-7)',
        day_name STRING COMMENT 'Day name',
        data_quality_score INT COMMENT 'Data quality score (0-100)',
        brand_valid BOOLEAN COMMENT 'Brand validation flag',
        market_valid BOOLEAN COMMENT 'Market validation flag',
        location_valid BOOLEAN COMMENT 'Location validation flag',
        traffic_category STRING COMMENT 'Traffic size category (LOW/MEDIUM/HIGH/VERY_HIGH)',
        time_of_day_category STRING COMMENT 'Time category (MORNING/AFTERNOON/EVENING/NIGHT)',
        is_weekend BOOLEAN COMMENT 'Weekend flag',
        gold_processed_at TIMESTAMP COMMENT 'Gold processing timestamp'
    ) USING DELTA
    PARTITIONED BY (Brand, Market, event_date_str)
    COMMENT 'Gold layer with business data only (no Kafka metadata), primary key: customer_id + latitude + longitude'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    print(f"✅ Created Gold table: {catalog}.{gold_database}.gld_traffic_data_enriched")
else:
    print(f"✅ Gold table already exists: {catalog}.{gold_database}.gld_traffic_data_enriched")

# Customer location dimension table
customer_location_exists = check_table_exists(catalog, gold_database, "gld_customer_locations")

if not customer_location_exists:
    print(f"\nCreating Gold table schema: {catalog}.{gold_database}.gld_customer_locations")
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{gold_database}.gld_customer_locations (
        customer_id STRING COMMENT 'Customer ID (primary key component)',
        latitude DOUBLE COMMENT 'Latitude coordinate (primary key component)',
        longitude DOUBLE COMMENT 'Longitude coordinate (primary key component)',
        address STRING COMMENT 'Physical address'
    ) USING DELTA
    COMMENT 'Customer location dimension table - unique customer_id + latitude + longitude combinations'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """)
    
    print(f"✅ Created Gold table: {catalog}.{gold_database}.gld_customer_locations")
else:
    print(f"✅ Gold table already exists: {catalog}.{gold_database}.gld_customer_locations")

# End of gold table creation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("SETUP COMPLETE")
print("="*80)

print(f"\n✅ Databases verified/created:")
print(f"   - {catalog}.{bronze_database}")
print(f"   - {catalog}.{silver_database}")
print(f"   - {catalog}.{gold_database}")
print(f"   - {catalog}.{metadata_database}")

print(f"\n✅ Tables verified/created:")
print(f"   Bronze Layer:")
print(f"   - {catalog}.{bronze_database}.kafka_data_bronze (source - should already exist)")

print(f"\n   Silver Layer:")
print(f"   - {catalog}.{silver_database}.slv_traffic_data_cleaned")
print(f"   - {catalog}.{silver_database}.slv_customer_engagement_summary")

print(f"\n   Gold Layer:")
print(f"   - {catalog}.{gold_database}.gld_traffic_data_enriched")
print(f"   - {catalog}.{gold_database}.gld_customer_locations")

print(f"\n   Metadata:")
print(f"   - {catalog}.{metadata_database}.checkpoints")

print("="*80)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

