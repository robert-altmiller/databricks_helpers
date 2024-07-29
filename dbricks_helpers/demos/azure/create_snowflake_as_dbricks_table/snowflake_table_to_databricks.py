# Databricks notebook source
# MAGIC %md
# MAGIC # Create Snowflake Table as Managed Table in Databricks Documentation:
# MAGIC ### https://learn.microsoft.com/en-us/azure/databricks/external-data/snowflake

# COMMAND ----------

# DBTITLE 1,Local Parameters
# databricks parameters
dbricks_catalog = "my-catalog"
dbricks_schema = "my-schema"

# snowflake parameters
snow_hostname = "snow-host"
snow_port = "443",
snow_username = "snow-username",
snow_password = "snow-password",
snow_warehouse_name = "snow-warehouse",
snow_database_name = "snow-database",
snow_schema_name = "snow-schema",
snowflake_tables_dict = {
    "snow_table1": ["snow-table1-partition1", "snow-table1-partition2"],
    "snow_table2": ["snow-table2-partition1", "snow-table2-partition2"]
}

# COMMAND ----------

# DBTITLE 1,Create a Snowflake Table in Databricks
# the following example applies to Databricks Runtime 11.3 LTS and above.

def get_snowflake_data(
  hostname = None,
  port = None,
  username = None,
  password = None,
  warehouse_name = None,
  database_name = None,
  schema_name = None,
  table_name = None
):
  """get data from a snowflake table"""
  snowflake_table = (spark.read
    .format("snowflake")
    .option("host", hostname)
    .option("port", port) # Optional - will use default port 443 if not specified.
    .option("user", username)
    .option("password", password)
    .option("sfWarehouse", warehouse_name)
    .option("database", database_name)
    .option("schema", schema_name) # Optional - will use default schema "public" if not specified.
    .option("dbtable", table_name)
    .load()
  )
  return snowflake_table

# COMMAND ----------

# DBTITLE 1,Function to Create Delta Managed Table
def write_delta_table(df = None, mode = "overwrite", partition_by = None, catalog = None, schema = None, table = None):
    """create a New Unity Catalog Managed Delta Table with or without partition"""
    if len(partition_by) > 0 and partition_by != None:
        df.write \
            .mode(mode) \
            .format("delta") \
            .partitionBy(*partition_by) \
            .saveAsTable(f"{catalog}.{schema}.{table}")
    else:
        df.write \
            .mode(mode) \
            .format("delta") \
            .saveAsTable(f"{catalog}.{schema}.{table}") 

# COMMAND ----------

# DBTITLE 1,Create Snowflake Table as a Managed Delta Table
for snow_table, snow_partition in snowflake_tables_dict.items():

    df_snowflake = get_snowflake_data(
        host_name = host_name,
        port = snow_port,
        username = snow_username,
        password = snow_password,
        warehouse_name = snow_warehouse,
        database_name = snow_database_name,
        schema_name = snow_schema_name,
        table_name = snow_table_name
    )
    
    write_delta_table(df_snowflake, mode = "overwrite", partition_by = snow_partition, catalog = dbricks_catalog, schema = dbricks_schema, table = snow_table)