# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka to Postgres Ingestion - Hybrid Approach
# MAGIC 
# MAGIC Uses Spark Streaming for Kafka (checkpoint management) + Direct Postgres writes (no serialization issues)

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet
# MAGIC %pip install --upgrade databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import psycopg2
import uuid
import json
import os
import time
from datetime import datetime
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Widget for config path
dbutils.widgets.text("config_path", "config/config.json", "Config File Path")
config_path = dbutils.widgets.get("config_path")

# Get the notebook's location and find config file
cwd = os.getcwd()
possible_paths = [
    os.path.join(cwd, config_path),
    os.path.join(os.path.dirname(cwd), config_path),
    os.path.join('..', config_path)
]

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    if '/notebooks/' in notebook_path:
        pipeline_root = notebook_path.split('/notebooks/')[0]
        if not pipeline_root.startswith('/Workspace'):
            pipeline_root = '/Workspace' + pipeline_root
        workspace_config_path = os.path.join(pipeline_root, config_path)
        possible_paths.append(workspace_config_path)
except:
    pass

print(f"🔍 Searching for config file...")
config_full_path = None
for path in possible_paths:
    abs_path = os.path.abspath(path)
    if os.path.exists(abs_path):
        config_full_path = abs_path
        print(f"   ✅ Found: {abs_path}")
        break

if not config_full_path:
    raise FileNotFoundError(f"Could not find config file: {config_path}")

with open(config_full_path, 'r') as f:
    config_data = json.load(f)

postgres_conn = config_data.get('postgres_connection', {})
kafka_source = config_data.get('kafka_source', {})
streaming_config = config_data.get('streaming_config', {})

# Build full table name with schema
postgres_schema = postgres_conn.get('postgres_schema', 'public')
postgres_table_name = postgres_conn.get('postgres_table')
full_table_name = f"{postgres_schema}.{postgres_table_name}"

CONFIG = {
    "postgres_instance_name": postgres_conn.get('postgres_instance_name'),
    "postgres_database": postgres_conn.get('postgres_database'),
    "postgres_schema": postgres_schema,
    "postgres_table": full_table_name,  # gold_db.kafka_data
    "kafka_topic": kafka_source.get('kafka_topic'),
    "kafka_secret_scope": kafka_source.get('kafka_secret_scope'),
    "kafka_secret_key": kafka_source.get('kafka_secret_key'),
    "kafka_security_protocol": kafka_source.get('kafka_security_protocol', 'PLAINTEXT'),
    "checkpoint_location": streaming_config.get('checkpoint_location'),
    "trigger_interval": streaming_config.get('trigger_interval', '10 seconds'),
    "batch_size": 500  # Process 500 records at a time
}

print("\n" + "="*70)
print("CONFIGURATION LOADED")
print("="*70)
print(f"Postgres Instance: {CONFIG['postgres_instance_name']}")
print(f"Postgres Schema: {CONFIG['postgres_schema']}")
print(f"Postgres Table: {CONFIG['postgres_table']}")
print(f"Kafka Topic: {CONFIG['kafka_topic']}")
print(f"Batch Size: {CONFIG['batch_size']}")
print(f"Checkpoint: {CONFIG['checkpoint_location']}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Connections

# COMMAND ----------

ws_client = WorkspaceClient()
user_email = ws_client.current_user.me().user_name
kafka_bootstrap_servers = dbutils.secrets.get(
    scope=CONFIG["kafka_secret_scope"],
    key=CONFIG["kafka_secret_key"]
)
db_instance = ws_client.database.get_database_instance(
    name=CONFIG["postgres_instance_name"]
)
postgres_host = f"instance-{db_instance.uid}.database.cloud.databricks.com"

# Get workspace URL and PAT token
workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Store in Spark config so foreachBatch can access without serialization issues
spark.conf.set("custom.postgres.host", postgres_host)
spark.conf.set("custom.postgres.user", user_email)
spark.conf.set("custom.postgres.instance", CONFIG["postgres_instance_name"])
spark.conf.set("custom.postgres.database", CONFIG["postgres_database"])
spark.conf.set("custom.postgres.schema", CONFIG["postgres_schema"])
spark.conf.set("custom.postgres.table", CONFIG["postgres_table"])
spark.conf.set("custom.workspace.url", workspace_url)
spark.conf.set("custom.workspace.token", pat_token)

print(f"✅ User: {user_email}")
print(f"✅ Postgres Host: {postgres_host}")
print(f"✅ Postgres Schema: {CONFIG['postgres_schema']}")
print(f"✅ Config stored in Spark conf for foreachBatch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions (for setup only, not used in foreachBatch)

# COMMAND ----------

def create_table():
    """Create schema and table if not exists - runs once at setup"""
    token = ws_client.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[CONFIG["postgres_instance_name"]]
    ).token
    
    conn = psycopg2.connect(
        host=postgres_host,
        port=5432,
        dbname=CONFIG["postgres_database"],
        user=user_email,
        password=token,
        sslmode="require"
    )
    cursor = conn.cursor()
    
    # Create schema first
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {CONFIG['postgres_schema']}"
    cursor.execute(create_schema_sql)
    print(f"✅ Schema '{CONFIG['postgres_schema']}' ready")
    
    # Create table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {CONFIG["postgres_table"]} (
        email TEXT,
        address TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        kafka_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP
    )
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Table '{CONFIG['postgres_table']}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Table

# COMMAND ----------

create_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Infer Schema from Kafka

# COMMAND ----------

print("🔍 Inferring schema from Kafka...")
sample_df = (
    spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", CONFIG["kafka_topic"])
        .option("kafka.security.protocol", CONFIG["kafka_security_protocol"])
        .option("startingOffsets", "earliest")
        .load()
        .select(col("value").cast("string"))
        .limit(1)
)

sample_json = sample_df.first()
if not sample_json or not sample_json["value"]:
    raise Exception("No messages found in Kafka topic")

json_schema = schema_of_json(sample_json["value"])
print(f"✅ Schema inferred")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Micro-batch Writer Function

# COMMAND ----------

def write_batch_to_postgres(batch_df, batch_id):
    """
    Write micro-batch to Postgres using JDBC (Official Databricks Pattern)
    Based on create_oltp_db_table_from_df from databricks_helpers
    """
    import time
    import uuid
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    
    start_time = time.time()
    
    print(f"\n{'='*60}")
    print(f"Batch {batch_id}: Processing records")
    
    try:
        # Get config from Spark conf (no serialization issues!)
        spark_session = batch_df.sparkSession
        postgres_host = spark_session.conf.get("custom.postgres.host")
        postgres_user = spark_session.conf.get("custom.postgres.user")
        postgres_instance = spark_session.conf.get("custom.postgres.instance")
        postgres_database = spark_session.conf.get("custom.postgres.database")
        postgres_table = spark_session.conf.get("custom.postgres.table")
        workspace_url = spark_session.conf.get("custom.workspace.url")
        workspace_token = spark_session.conf.get("custom.workspace.token")
        
        # Create WorkspaceClient with explicit config
        ws_config = Config(
            host=workspace_url,
            token=workspace_token
        )
        ws_client_batch = WorkspaceClient(config=ws_config)
        
        # Get database instance details
        db_instance = ws_client_batch.database.get_database_instance(name=postgres_instance)
        
        # Build JDBC URL (using official pattern)
        jdbc_url = f"jdbc:postgresql://instance-{db_instance.uid}.database.cloud.databricks.com:5432/{postgres_database}?sslmode=require"
        
        # Get fresh token for this batch
        token = ws_client_batch.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[postgres_instance]
        ).token
        
        # JDBC connection properties (official pattern)
        connection_properties = {
            "user": postgres_user,
            "password": token,
            "driver": "org.postgresql.Driver"
        }
        
        # Write using JDBC (Databricks official method)
        # This is distributed across executors for performance
        batch_df.write \
            .option("stringtype", "unspecified") \
            .jdbc(
                url=jdbc_url,
                table=postgres_table,
                mode="append",
                properties=connection_properties
            )
        
        elapsed = time.time() - start_time
        record_count = batch_df.count()
        
        print(f"   ✅ Inserted {record_count} records in {elapsed:.2f}s")
        print(f"   ⚡ Throughput: {record_count/elapsed:.0f} records/sec")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"   ❌ Error in batch {batch_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Spark Streaming with Micro-batches

# COMMAND ----------

print("="*70)
print("STARTING STREAMING INGESTION")
print("="*70)
print(f"📥 Source: Kafka topic '{CONFIG['kafka_topic']}'")
print(f"📤 Target: Postgres table '{CONFIG['postgres_table']}'")
print(f"📦 Batch size: {CONFIG['batch_size']} records")
print(f"⏱️  Trigger: {CONFIG['trigger_interval']}")
print(f"💾 Checkpoint: {CONFIG['checkpoint_location']}")
print("="*70)

# Read from Kafka using Spark Streaming
kafka_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", CONFIG["kafka_topic"])
        .option("kafka.security.protocol", CONFIG["kafka_security_protocol"])
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", CONFIG["batch_size"])  # 500 records per trigger
        .option("failOnDataLoss", "false")
        .load()
)

# Parse JSON and select fields
parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), json_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
)

# Select only the 4 fields we need
final_stream = parsed_stream.select(
    col("data.email").alias("email"),
    col("data.address").alias("address"),
    col("data.latitude").alias("latitude"),
    col("data.longitude").alias("longitude"),
    col("kafka_timestamp"),
    current_timestamp().alias("ingestion_timestamp")
)

# Start streaming with foreachBatch
query = (
    final_stream.writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CONFIG["checkpoint_location"])
        .trigger(processingTime=CONFIG["trigger_interval"])
        .start()
)

print("\n✅ Streaming query started!")
print(f"📍 Query ID: {query.id}")
print(f"📍 Run ID: {query.runId}")
print(f"\n🔄 Processing micro-batches of {CONFIG['batch_size']} records every {CONFIG['trigger_interval']}")
print("   Data flows: Kafka → Spark (checkpoint) → Collect → Postgres")
print("\n   Press Ctrl+C or stop the notebook to terminate\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream

# COMMAND ----------

print("\n📊 Stream Status:")
print(f"   Active: {query.isActive}")
print(f"   ID: {query.id}")

# Keep running (for jobs, uncomment this)
# query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop Stream (Optional)

# COMMAND ----------

# Uncomment to stop
# query.stop()
# print("✅ Stream stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Postgres Data (Optional)

# COMMAND ----------

# Uncomment to check data
# conn = get_postgres_connection()
# cursor = conn.cursor()
# cursor.execute(f"SELECT COUNT(*) FROM {CONFIG['postgres_table']}")
# count = cursor.fetchone()[0]
# print(f"📊 Total records: {count}")
# cursor.execute(f"SELECT * FROM {CONFIG['postgres_table']} ORDER BY ingestion_timestamp DESC LIMIT 5")
# print("\n📝 Latest 5 records:")
# for row in cursor.fetchall():
#     print(f"   {row[0][:30]}... | Lat: {row[2]} | Lon: {row[3]}")
# cursor.close()
# conn.close()
