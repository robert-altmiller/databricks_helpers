# Databricks notebook source
# DBTITLE 1,Library Imports
import json, os

# COMMAND ----------

# DBTITLE 1,Local UC Parameters
catalog = "hive_metastore"
schema = "default"

# COMMAND ----------

# DBTITLE 1,Create Some Data in Hive Metastore
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Create Baggage Table + Data
spark.sql(""" 
CREATE OR REPLACE TABLE baggage (
    baggage_id INT,
    ticket_id INT,
    weight FLOAT,
    baggage_type STRING
);          
""")

spark.sql("""
INSERT INTO baggage (baggage_id, ticket_id, weight, baggage_type)
VALUES
    (1, 1, 23.50, 'Checked'),
    (3, 3, 18.00, 'Checked'),
    (4, 4, 5.50, 'Carry-on'),
    (2, 2, 7.00, 'Checked on different flight');
""")

# COMMAND ----------

# DBTITLE 1,Local Hive Parameters
try:
    # Extract values from Spark config
    metastore_url = spark.sparkContext._jsc.hadoopConfiguration().get("javax.jdo.option.ConnectionURL")
    print(f"metastore_url: {metastore_url}")

    metastore_url_port = metastore_url.split("//")[1].split("/")[0]
    print(f"metastore_url_port: {metastore_url_port}")

    hostname_url = metastore_url_port.split(":")[0]
    print(f"hostname_url: {hostname_url}")

    hostname_port = metastore_url_port.split(":")[1]
    print(f"hostname_port: {hostname_port}")

    hostname_database = (metastore_url.split("//")[1].split("/")[1]).split("?")[0]
    print(f"hostname_database: {hostname_database}")

    hostname_user = spark.sparkContext._jsc.hadoopConfiguration().get("javax.jdo.option.ConnectionUserName")
    print(f"hostname_user: {hostname_user}")

    hostname_password = spark.sparkContext._jsc.hadoopConfiguration().get("javax.jdo.option.ConnectionPassword")
    print(f"hostname_password: {hostname_password}")
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,Local DBFS Parameters
# fc = foreign catalog
connection_name = f"{catalog}_{schema}_fc"
print(f"connection_name: {connection_name}")

# Determine notebook path (e.g., "/Workspace/Users/you...")
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file_path = f"{notebook_path.rsplit('/', 1)[0]}/{connection_name}.json"
print(f"workspace_file_path: {workspace_file_path}")

# Convert to DBFS file path
dbfs_file_path = f"/Workspace{workspace_file_path}"
print(f"dbfs_file_path: {dbfs_file_path}")

# COMMAND ----------

# DBTITLE 1,Write External Hive Metastore Connection Details to JSON
# Bundle the config into a dictionary
config_data = {
    "metastore_url": metastore_url,
    "metastore_url_port": metastore_url_port,
    "hostname_url": hostname_url,
    "hostname_port": hostname_port,
    "hostname_database": hostname_database,
    "hostname_user": hostname_user,
    "hostname_password": hostname_password,
    "connection_name": connection_name
}

# Write JSON to the workspace path
with open(f"{dbfs_file_path}", "w+") as f:
    json.dump(config_data, f, indent=2)

print(f"✅ Config written to {dbfs_file_path}")

# COMMAND ----------

# DBTITLE 1,Read External Connection Metadata and Create New UC Connection
# Read config file
with open(dbfs_file_path, "r") as f:
    config = json.load(f)

print(config)

# Extract connection info
connection_name = config["connection_name"]
hostname_url = config["hostname_url"]
hostname_port = config["hostname_port"]
hostname_user = config["hostname_user"]
hostname_password = config["hostname_password"]
hostname_database = config["hostname_database"]

# Create the Hive Metastore connection (external)
# If you need to create an internal connection you will need to do this manually.
spark.sql(f"""
    CREATE CONNECTION IF NOT EXISTS {connection_name}
    TYPE hive_metastore
    OPTIONS (
        host '{hostname_url}',
        port '{hostname_port}',
        user '{hostname_user}',
        password '{hostname_password}',
        database '{hostname_database}',
        db_type 'MYSQL',
        version '2.3'
    )
""")

print(f"✅ Connection '{connection_name}' created successfully.")

# COMMAND ----------

# DBTITLE 1,Create Foreign Catalog For Hive Metastore
authorized_paths = "abfss://CONTAINER@STORAGEACCOUNTNAME.dfs.core.windows.net/"
spark.sql("""
    CREATE FOREIGN CATALOG IF NOT EXISTS {hive_metastore_default_fc}
    USING CONNECTION {connection_name}
    OPTIONS (authorized_paths '{authorized_paths}');
""")