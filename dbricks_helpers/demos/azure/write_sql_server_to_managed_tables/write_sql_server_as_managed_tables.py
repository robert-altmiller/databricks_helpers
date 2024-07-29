# Databricks notebook source
# DBTITLE 1,Library Imports
import ast
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Read Workflow Widgets
dbutils.widgets.text("jdbcHostname", "alt-sql-server.database.windows.net")
jdbcHostname = dbutils.widgets.get("jdbcHostname")
print(f"jdbcHostname: {jdbcHostname}")

dbutils.widgets.text("jdbcDatabase", "alt-sql-database")
jdbcDatabase = dbutils.widgets.get("jdbcDatabase")
print(f"jdbcDatabase: {jdbcDatabase}")

dbutils.widgets.text("jdbcPort", "1433")
jdbcPort = int(dbutils.widgets.get("jdbcPort"))
print(f"jdbcPort: {jdbcPort}")

dbutils.widgets.text("sqlschema", "dbo")
sqlschema = dbutils.widgets.get("sqlschema")
print(f"sqlschema: {sqlschema}")

dbutils.widgets.text("use_static_table_list", "False")
use_static_table_list = bool(dbutils.widgets.get("use_static_table_list"))
print(f"use_static_table_list: {use_static_table_list}")

dbutils.widgets.text("static_table_list", "['Customers']")
static_table_list = dbutils.widgets.get("static_table_list")
static_table_list = ast.literal_eval(static_table_list)
print(f"static_table_list: {static_table_list}")

dbutils.widgets.text("catalog", "hive_metastore")
catalog = dbutils.widgets.get("catalog")
print(f"UC catalog: {catalog}")

dbutils.widgets.text("schema", "altmiller")
schema = dbutils.widgets.get("schema")
print(f"UC schema: {schema}")

# COMMAND ----------

# DBTITLE 1,Local Parameters
# set up JDBC connection properties

# jdbcHostname = 'alt-sql-server.database.windows.net'
# jdbcDatabase = 'alt-sql-database'
# jdbcPort = 1433
jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}'

connectionProperties = {
  "user" : "robert.altmiller",
  "password" : "********",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# # list of tables to migrate
# sqlschema = "dbo"
# use_static_table_list = False
# static_table_list = ["Customers"]# ,"ExpiredCustomers"]#, "Products"]


# catalog name
# catalog = "hive_metastore"
# schema = "altmiller"

# create Databricks Catalog and Schema
try: # create catalog
    spark.sql(f"CREATE CATALOG {catalog}")
except: 
  print(f"could not create catalog {catalog}")
spark.sql(f"USE CATALOG {catalog}")

try: # create schema
    spark.sql(f"CREATE SCHEMA {schema}")
except: print(f"could not create schema {schema}\n")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# DBTITLE 1,Remove UC Managed Tables For Demo Purposes
# spark.sql(f"DROP TABLE `{catalog}`.`{schema}`.customers")
# spark.sql(f"DROP TABLE `{catalog}`.`{schema}`.expiredcustomers")
# spark.sql(f"DROP TABLE `{catalog}`.`{schema}`.products")

# COMMAND ----------

# DBTITLE 1,Delta Table Helper Functions
def write_delta_table(df = None, mode = "overwrite", partition_by = None, catalog = None, schema = None, table = None):
    """
    create a New Unity Catalog Managed Delta Table with or without partition
    mode = "append" or "overwrite"
    """
    if partition_by != None:
        df.write \
            .mode(mode) \
            .option("inferSchema", "true") \
            .format("delta") \
            .partitionBy(*partition_by) \
            .saveAsTable(f"{catalog}.{schema}.{table}")
    else:
        df.write \
            .mode(mode) \
            .option("inferSchema", "true") \
            .format("delta") \
            .saveAsTable(f"{catalog}.{schema}.{table}")


def check_csv_df_for_header(csv_dir_path = None, data_type = None):
    """checks a spark dataframe built from csvs if header is needed"""
    # Here, we're checking if the first value in the row is a string.
    print(csv_dir_path)
    df = spark.read.load(csv_dir_path, format = data_type)
    first_row = df.head(1)[0]
    # determine if header should be set to true or false
    header = all(isinstance(value, str) for value in first_row)
    return header


def read_sql_server_data(jdbc_url = None, table_name = None, connection_props = None):
    """read table into Spark dataframe from SQL server"""
    return spark.read.jdbc(url = jdbc_url, table = table_name, properties = connection_props)


def get_sql_server_table_pk(jdbc_url = None, schema = None, table_name = None, connection_props = None):
    """get sql server table primary key (pk)"""
    query = f"""
        (SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
        ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.TABLE_SCHEMA = '{schema}' AND tc.TABLE_NAME = '{table_name}') AS primary_key_info
    """
    df = read_sql_server_data(jdbc_url, query, connection_props).toPandas()
    return {name.lower(): list(group['COLUMN_NAME']) for name, group in df.groupby('TABLE_NAME')}


def read_source_data(dir_path = None, data_type = None, connection_props = None):
    """
    read different types of data into spark dataframe
    read csv, parquet, delta, and sql_server
    """
    if data_type.lower() == "csv" and check_csv_df_for_header(dir_path, data_type) == True:
        return spark.read.load(dir_path, format = data_type, header = True)
    else: return spark.read.load(dir_path, format = data_type)


def remove_spaces_from_column_headers(df = None):
    """iterate over column names and remove spaces in spark dataframe"""
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "")  # Remove spaces
        df = df.withColumnRenamed(col_name, new_col_name)
    return df


def check_table_exists(catalog = None, schema = None, table_name = None):
    """check if Delta table exists"""
    try:
        full_table_name = f"{catalog}.{schema}.{table_name}"
        df = spark.sql(f"SHOW TABLES IN {schema} LIKE '{table_name}'")
        return df.filter(df.tableName == table_name).count() > 0
    except AnalysisException as e:
        # log the exception
        print(f"An error occurred: {str(e)}")
        return False

# COMMAND ----------

# DBTITLE 1,Write and Merge Delta Tables
# query to fetch all table names from the SQL Server database or use static list of tables
if use_static_table_list == False:
    query = "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE') AS table_names"
    tables_df = read_sql_server_data(jdbcUrl, query, connectionProperties)
else: 
    rows = [(table_name,) for table_name in static_table_list]
    tables_df = spark.createDataFrame(rows, schema=["TABLE_NAME"])

# read the list of tables using JDBC
table_names = [row.TABLE_NAME for row in tables_df.collect()]

# Loop through table names, read each table and write it to Databricks as a managed table
for table_name in table_names:
    
    table_name = f"{table_name.lower()}"
    print(f"check if '{table_name}' exists in catalog '{catalog}' and schema '{schema}'")
    table_exists = check_table_exists(catalog, schema, table_name)
    print(f"{catalog}.{schema}.{table_name} table already exists: {table_exists}")
    
    if not table_exists: # create brand new table in UC schema

        print(f"creating UC table '{catalog}.{schema}.{table_name}'....")
        table_df = read_sql_server_data(jdbcUrl, table_name, connectionProperties)
        table_df = remove_spaces_from_column_headers(table_df)     
        write_delta_table(
            df = table_df, 
            mode = "overwrite", 
            partition_by = None, 
            catalog = catalog, 
            schema = schema, 
            table = table_name
        )
        print(f"finished creating Unity Catalog managed table '{catalog}.{schema}.{table_name}'....\n")
    
    else: # merge upsert table to source since UC table already exists

        target_table_path = f"{catalog}.{schema}.{table_name}"
        target_df = DeltaTable.forName(spark, target_table_path)
        source_df = read_sql_server_data(jdbcUrl, table_name, connectionProperties)

        # define the merge condition using all columns as the key
        col_pk_list = get_sql_server_table_pk(jdbcUrl, sqlschema, table_name, connectionProperties)[table_name]
        update_columns = {f"target.{col}": f"source.{col}" for col in source_df.columns if col not in str(col_pk_list)}
        print(f"update_columns: {update_columns}")
        merge_condition = ' AND '.join([f"target.{col_pk} = source.{col_pk}" for col_pk in col_pk_list])
        print(f"merge_condition: {merge_condition}")

        # perform the merge operation
        print(f"merging Unity Catalog managed Delta table for delta table '{catalog}.{schema}.{table_name}'....")
        merge_result = (
            target_df.alias("target")
            .merge(
                source_df.alias("source"),
                condition = merge_condition
            )
            .whenMatchedUpdate(set = update_columns)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"finished merging Unity Catalog managed Delta table for delta table '{catalog}.{schema}.{table_name}'....\n")
        merge_condition = None

# COMMAND ----------

# DBTITLE 1,Check UC Managed Tables Data Before
df = spark.sql("SELECT * FROM `hive_metastore`.`altmiller`.`customers` ORDER BY CustomerID")
display(df)

# COMMAND ----------

# DBTITLE 1,Check UC Managed Tables Data After
df = spark.sql("SELECT * FROM `hive_metastore`.`altmiller`.`customers` ORDER BY CustomerID")
display(df)

# COMMAND ----------


