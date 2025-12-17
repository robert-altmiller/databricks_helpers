# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Stats Generator
# MAGIC 
# MAGIC Shows ALL customers from live Kafka data:
# MAGIC - **gold_db.kafka_data** (live Kafka data - PRIMARY source)
# MAGIC - **gold_db.traffic_data_enriched_pg** (ETL processed data - for brand info)
# MAGIC 
# MAGIC Includes new customers not yet in ETL table (brand = UNKNOWN)

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

postgres_schema = postgres_conn.get('postgres_schema', 'public')

CONFIG = {
    "postgres_instance_name": postgres_conn.get('postgres_instance_name'),
    "postgres_database": postgres_conn.get('postgres_database'),
    "postgres_schema": postgres_schema,
    "kafka_table": f"{postgres_schema}.kafka_data",
    "traffic_table": f"{postgres_schema}.traffic_data_enriched_pg",
    "stats_table": f"{postgres_schema}.customer_stats",
    "stats_interval": 60  # Generate stats every 60 seconds
}

print("\n" + "="*70)
print("CUSTOMER STATS CONFIGURATION")
print("="*70)
print(f"Postgres Instance: {CONFIG['postgres_instance_name']}")
print(f"Postgres Schema: {CONFIG['postgres_schema']}")
print(f"Kafka Table: {CONFIG['kafka_table']}")
print(f"Traffic Table: {CONFIG['traffic_table']}")
print(f"Stats Table: {CONFIG['stats_table']}")
print(f"Stats Interval: {CONFIG['stats_interval']} seconds")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Connections

# COMMAND ----------

ws_client = WorkspaceClient()
user_email = ws_client.current_user.me().user_name
db_instance = ws_client.database.get_database_instance(
    name=CONFIG["postgres_instance_name"]
)
postgres_host = f"instance-{db_instance.uid}.database.cloud.databricks.com"

print(f"✅ User: {user_email}")
print(f"✅ Postgres Host: {postgres_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_postgres_connection():
    """Get Postgres connection with fresh token"""
    token = ws_client.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[CONFIG["postgres_instance_name"]]
    ).token
    
    return psycopg2.connect(
        host=postgres_host,
        port=5432,
        dbname=CONFIG["postgres_database"],
        user=user_email,
        password=token,
        sslmode="require"
    )

def create_stats_table():
    """Create simple customer stats table"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {CONFIG['stats_table']} (
        stat_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        customers_last_5_mins INTEGER,
        customers_last_1_hour INTEGER,
        customers_last_7_days INTEGER,
        customers_last_30_days INTEGER,
        total_customers INTEGER,
        gap_customers INTEGER,
        athleta_customers INTEGER,
        old_navy_customers INTEGER,
        banana_republic_customers INTEGER,
        PRIMARY KEY (stat_timestamp)
    )
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Stats table '{CONFIG['stats_table']}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Stats Table

# COMMAND ----------

create_stats_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer Stats Function

# COMMAND ----------

def generate_customer_stats():
    """
    Generate customer onboarding statistics
    Finds customers from kafka_data that exist in traffic_data_enriched_pg
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Check if traffic table exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '{CONFIG['postgres_schema']}' 
                AND table_name = 'traffic_data_enriched_pg'
            )
        """)
        traffic_exists = cursor.fetchone()[0]
        
        if traffic_exists:
            # Show ALL customers from Kafka (including new ones not yet in traffic table)
            stats_sql = f"""
            WITH all_customers AS (
                SELECT DISTINCT 
                    k.email, 
                    k.ingestion_timestamp, 
                    COALESCE(t."Brand", 'UNKNOWN') as brand
                FROM {CONFIG['kafka_table']} k
                LEFT JOIN {CONFIG['traffic_table']} t ON k.email = t.email
            )
            INSERT INTO {CONFIG['stats_table']} 
            (stat_timestamp, customers_last_5_mins, customers_last_1_hour, customers_last_7_days, 
             customers_last_30_days, total_customers,
             gap_customers, athleta_customers, old_navy_customers, banana_republic_customers)
            SELECT 
                NOW() as stat_timestamp,
                COUNT(DISTINCT CASE WHEN ingestion_timestamp >= NOW() - INTERVAL '5 minutes' THEN email END),
                COUNT(DISTINCT CASE WHEN ingestion_timestamp >= NOW() - INTERVAL '1 hour' THEN email END),
                COUNT(DISTINCT CASE WHEN ingestion_timestamp >= NOW() - INTERVAL '7 days' THEN email END),
                COUNT(DISTINCT CASE WHEN ingestion_timestamp >= NOW() - INTERVAL '30 days' THEN email END),
                COUNT(DISTINCT email),
                COUNT(DISTINCT CASE WHEN UPPER(brand) = 'GAP' THEN email END),
                COUNT(DISTINCT CASE WHEN UPPER(brand) = 'ATHLETA' THEN email END),
                COUNT(DISTINCT CASE WHEN UPPER(brand) = 'OLD NAVY' THEN email END),
                COUNT(DISTINCT CASE WHEN UPPER(brand) = 'BANANA REPUBLIC' THEN email END)
            FROM all_customers
            """
        
        cursor.execute(stats_sql)
        conn.commit()
        
        # Display the latest stats
        cursor.execute(f"""
            SELECT * FROM {CONFIG['stats_table']} 
            ORDER BY stat_timestamp DESC 
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        if row:
            print(f"\n📊 {row[0].strftime('%H:%M:%S')} | 5m:{row[1]:2d} 1h:{row[2]:3d} 7d:{row[3]:4d} 30d:{row[4]:5d} Total:{row[5]:5d} | GAP:{row[6]} Athleta:{row[7]} OldNavy:{row[8]} BR:{row[9]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        conn.rollback()
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous Stats Generation Loop

# COMMAND ----------

print("="*70)
print("CUSTOMER STATS GENERATOR")
print("="*70)
print(f"Stats Table: {CONFIG['stats_table']}")
print(f"Interval: Every {CONFIG['stats_interval']} seconds")
print(f"Source: {CONFIG['kafka_table']} (ALL customers)")
print(f"Brand Info: {CONFIG['traffic_table']} (LEFT JOIN)")
print("="*70)

stats_generated = 0
last_token_refresh = time.time()
token_refresh_interval = 3600  # Refresh connection every hour

try:
    while True:
        try:
            # Refresh connection if needed
            if time.time() - last_token_refresh > token_refresh_interval:
                print("🔄 Refreshing connection...")
                last_token_refresh = time.time()
            
            # Generate stats
            generate_customer_stats()
            stats_generated += 1
            
            # Wait for next interval
            time.sleep(CONFIG['stats_interval'])
            
        except Exception as e:
            print(f"⚠️  Error in stats generation: {str(e)}")
            print(f"   Retrying in {CONFIG['stats_interval']} seconds...")
            time.sleep(CONFIG['stats_interval'])
            continue

except KeyboardInterrupt:
    print(f"\n{'='*70}")
    print("⏹️  STATS GENERATOR STOPPED")
    print(f"Total stats generated: {stats_generated}")
    print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Stats History

# COMMAND ----------

# Uncomment to view recent stats
# conn = get_postgres_connection()
# cursor = conn.cursor()
# cursor.execute(f"""
#     SELECT stat_timestamp, customers_last_5_mins, customers_last_1_hour, 
#            customers_last_7_days, customers_last_30_days, total_customers,
#            gap_customers, athleta_customers, old_navy_customers, banana_republic_customers
#     FROM {CONFIG['stats_table']}
#     ORDER BY stat_timestamp DESC
#     LIMIT 20
# """)
# print("📊 STATS HISTORY (Last 20 runs)")
# print("="*70)
# for row in cursor.fetchall():
#     print(f"{row[0]} | 5m:{row[1]:2d} 1h:{row[2]:3d} 7d:{row[3]:4d} 30d:{row[4]:5d} Tot:{row[5]:5d} | GAP:{row[6]} Ath:{row[7]} ON:{row[8]} BR:{row[9]}")
# print("="*70)
# cursor.close()
# conn.close()

