# Databricks notebook source
# MAGIC %md
# MAGIC # Insert Sample Data to Bronze Table
# MAGIC 
# MAGIC Inserts 10 sample records into kafka_data_bronze table

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import json
import builtins

# Get parameters
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("bronze_database", "", "Bronze Database")
dbutils.widgets.text("bronze_table", "", "Bronze Table")

catalog = dbutils.widgets.get("catalog")
bronze_database = dbutils.widgets.get("bronze_database")
bronze_table = dbutils.widgets.get("bronze_table")

print(f"Catalog: {catalog}")
print(f"Bronze DB: {bronze_database}")
print(f"Bronze Table: {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Table if not exists

# COMMAND ----------

# Drop and recreate bronze table to ensure correct schema
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{bronze_database}.{bronze_table}")
print(f"🗑️  Dropped existing table: {catalog}.{bronze_database}.{bronze_table}")

# Create bronze table with correct schema
spark.sql(f"""
CREATE TABLE {catalog}.{bronze_database}.{bronze_table} (
    customer_id STRING,
    email STRING,
    address STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    Brand STRING,
    Market STRING,
    traffic_count INT,
    event_date TIMESTAMP,
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    timestampType INT,
    key STRING,
    value STRING,
    headers ARRAY<STRUCT<key: STRING, value: BINARY>>,
    ingestion_timestamp TIMESTAMP
) USING DELTA
COMMENT 'Bronze layer for Kafka data'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")

print(f"✅ Bronze table created/verified: {catalog}.{bronze_database}.{bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data

# COMMAND ----------

import random
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType, ArrayType, BinaryType

# Define schema explicitly
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("Brand", StringType(), True),
    StructField("Market", StringType(), True),
    StructField("traffic_count", IntegerType(), True),
    StructField("event_date", TimestampType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("timestampType", IntegerType(), True),
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
    StructField("headers", ArrayType(StructType([
        StructField("key", StringType(), True),
        StructField("value", BinaryType(), True)
    ])), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])

# Sample data
brands = ["GAP", "Old Navy", "Athleta", "Banana Republic"]
markets = ["US", "CA", "EU", "APAC"]
base_time = datetime.now()

sample_data = []
for i in range(10):
    customer_id = f"CUST{1000 + i}"
    email = f"customer{i+1}@example.com"
    brand = random.choice(brands)
    market = random.choice(markets)
    
    # Generate realistic coordinates
    lat_base = 37.7749 if market == "US" else (43.6532 if market == "CA" else (51.5074 if market == "EU" else 35.6762))
    lon_base = -122.4194 if market == "US" else (-79.3832 if market == "CA" else (-0.1278 if market == "EU" else 139.6503))
    
    latitude = builtins.round(lat_base + random.uniform(-0.5, 0.5), 6)
    longitude = builtins.round(lon_base + random.uniform(-0.5, 0.5), 6)
    
    event_time = base_time - timedelta(hours=i)
    
    record = (
        customer_id,
        email,
        f"{random.randint(100, 9999)} Main Street, City, State",
        latitude,
        longitude,
        brand,
        market,
        random.randint(1, 100),
        event_time,
        "jomin_johny_fe_tech_onboarding_kafka_test-4",
        random.randint(0, 2),
        1000 + i,
        event_time,
        0,
        customer_id,
        json.dumps({
            "customer_id": customer_id,
            "email": email,
            "Brand": brand,
            "Market": market,
            "latitude": latitude,
            "longitude": longitude
        }),
        [],
        datetime.now()
    )
    sample_data.append(record)

# Create DataFrame with explicit schema
df = spark.createDataFrame(sample_data, schema)

print(f"✅ Generated {len(sample_data)} sample records")
print("\nSample data preview:")
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Sample Data

# COMMAND ----------

# Insert data into bronze table
df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalog}.{bronze_database}.{bronze_table}")

print(f"✅ Inserted {len(sample_data)} records into {catalog}.{bronze_database}.{bronze_table}")

# Verify insert
record_count = spark.table(f"{catalog}.{bronze_database}.{bronze_table}").count()
print(f"✅ Total records in table: {record_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Inserted Data

# COMMAND ----------

# Show the inserted data
display(spark.table(f"{catalog}.{bronze_database}.{bronze_table}").orderBy(col("ingestion_timestamp").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("SAMPLE DATA INSERTION COMPLETE")
print("="*80)
print(f"\n✅ Table: {catalog}.{bronze_database}.{bronze_table}")
print(f"✅ Records inserted: {len(sample_data)}")
print(f"✅ Total records in table: {record_count}")
print(f"\n📊 Data breakdown:")
print(f"   - Brands: {', '.join(brands)}")
print(f"   - Markets: {', '.join(markets)}")
print(f"   - Customer IDs: CUST1000 to CUST1009")
print(f"   - Time range: Last {len(sample_data)} hours")
print("="*80)

dbutils.notebook.exit("SUCCESS")

