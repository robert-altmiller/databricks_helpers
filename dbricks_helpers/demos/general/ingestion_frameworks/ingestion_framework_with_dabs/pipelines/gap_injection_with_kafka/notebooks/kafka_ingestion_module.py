# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka Data Ingestion Module
# MAGIC 
# MAGIC Reusable module for Kafka to Delta Lake ingestion with streaming

# COMMAND ----------

import json
from pyspark.sql.functions import col, current_timestamp, from_json, schema_of_json
from pyspark.sql.types import StructType
import base64

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Create widgets for all parameters
dbutils.widgets.text("kafka_topic", "", "Kafka Topic")
dbutils.widgets.text("kafka_bootstrap_servers_secret_scope", "", "Kafka Secret Scope")
dbutils.widgets.text("kafka_bootstrap_servers_secret_key", "", "Kafka Secret Key")
dbutils.widgets.text("kafka_security_protocol", "PLAINTEXT", "Kafka Security Protocol")
dbutils.widgets.text("starting_offsets", "earliest", "Starting Offsets")
dbutils.widgets.text("target_catalog", "", "Target Catalog")
dbutils.widgets.text("target_schema", "", "Target Schema")
dbutils.widgets.text("target_table", "", "Target Table")
dbutils.widgets.text("checkpoint_location", "", "Checkpoint Location")
dbutils.widgets.dropdown("merge_schema", "true", ["true", "false"], "Enable Schema Evolution")

# Get parameters
kafka_topic = dbutils.widgets.get("kafka_topic")
kafka_bootstrap_servers_secret_scope = dbutils.widgets.get("kafka_bootstrap_servers_secret_scope")
kafka_bootstrap_servers_secret_key = dbutils.widgets.get("kafka_bootstrap_servers_secret_key")
kafka_security_protocol = dbutils.widgets.get("kafka_security_protocol")
starting_offsets = dbutils.widgets.get("starting_offsets")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
merge_schema = dbutils.widgets.get("merge_schema").lower() == "true"

print("=" * 80)
print("KAFKA INGESTION CONFIGURATION")
print("=" * 80)
print(f"Kafka Topic: {kafka_topic}")
print(f"Secret Scope: {kafka_bootstrap_servers_secret_scope}")
print(f"Secret Key: {kafka_bootstrap_servers_secret_key}")
print(f"Security Protocol: {kafka_security_protocol}")
print(f"Starting Offsets: {starting_offsets}")
print(f"Target: {target_catalog}.{target_schema}.{target_table}")
print(f"Checkpoint: {checkpoint_location}")
print(f"Merge Schema: {merge_schema}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Kafka Credentials

# COMMAND ----------

# Get Kafka bootstrap servers from secret
kafka_bootstrap_servers = dbutils.secrets.get(
    scope=kafka_bootstrap_servers_secret_scope,
    key=kafka_bootstrap_servers_secret_key
)

print(f"✅ Kafka servers loaded from secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Target Schema

# COMMAND ----------

# Create target database if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
print(f"✅ Schema {target_catalog}.{target_schema} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Infer Schema from Kafka

# COMMAND ----------

# Infer schema from a sample Kafka message
print("\nInferring schema from Kafka sample message...")

try:
    sample_df = (
        spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("kafka.security.protocol", kafka_security_protocol)
            .option("startingOffsets", "earliest")
            .option("kafka.request.timeout.ms", "60000")
            .option("kafka.session.timeout.ms", "60000")
            .load()
            .select(col("value").cast("string"))
            .limit(1)
    )
    
    # Get the schema from the sample JSON
    sample_json = sample_df.first()
    if sample_json and sample_json["value"]:
        json_schema = schema_of_json(sample_json["value"])
        print(f"✅ Schema inferred successfully from sample message")
    else:
        raise Exception("No messages found in Kafka topic to infer schema")
except Exception as e:
    print(f"❌ Error inferring schema: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream from Kafka to Delta

# COMMAND ----------

target_table_full = f"{target_catalog}.{target_schema}.{target_table}"
print(f"\n{'='*80}")
print(f"STARTING STREAMING INGESTION")
print(f"{'='*80}")
print(f"Source: Kafka topic '{kafka_topic}'")
print(f"Target: {target_table_full}")
print(f"{'='*80}\n")

# Streaming read from Kafka
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("kafka.security.protocol", kafka_security_protocol)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "60000")
        .load()
)

# Parse JSON value and flatten the structure
parsed_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("parsed_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_timestamp")
)

# Flatten the parsed JSON structure - extract all fields from parsed_value
final_df = parsed_df.select(
    "parsed_value.*",  # This expands all fields from the JSON
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "ingestion_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta with Trigger

# COMMAND ----------

# Write stream to Delta table - TRIGGER ONCE (process all available data then stop)
query = (
    final_df.writeStream
        .format("delta")
        .outputMode("append")  # Use append mode for streaming to Delta
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", str(merge_schema).lower())
        .trigger(availableNow=True)  # Process all available data then stop automatically
        .toTable(target_table_full)
)

# Wait for the stream to complete
print("⏳ Processing stream...")
query.awaitTermination()

print(f"\n{'='*80}")
print(f"STREAMING COMPLETED SUCCESSFULLY")
print(f"{'='*80}")
print(f"✅ Target table: {target_table_full}")
print(f"✅ Checkpoint: {checkpoint_location}")
print(f"✅ Stream processed all available data and stopped automatically")
print(f"{'='*80}\n")

# COMMAND ----------

# Get record count
record_count = spark.table(target_table_full).count()
print(f"📊 Total records in table: {record_count}")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

