# Databricks notebook source
# MAGIC %pip install psycopg2 --upgrade databricks-sdk tabulate
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
import os, json, psycopg2, uuid, re
from datetime import date
from tabulate import tabulate
from databricks.sdk import WorkspaceClient
import pyspark
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk.service.database import *
from databricks.sdk.service.catalog import *

# COMMAND ----------

# DBTITLE 1,Authoring Helpers
# MAGIC %run "./authoring_helpers"

# COMMAND ----------

# DBTITLE 1,Lakebase Helpers
# MAGIC %run "./lakebase_helpers"

# COMMAND ----------

# DBTITLE 1,Local Parameters
workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
print(f"workspace_url: {workspace_url}")

pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
print(f"pat_token: {pat_token}")

user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(f"user_email: {user_email}")

database_instance_name = "postgres-instance"
print(f"database_instance_name: {database_instance_name}")

postgres_database_name = "postgres_database"
print(f"postgres_database_name: {postgres_database_name}")

database_catalog_name = "postgres_catalog"
print(f"database_catalog_name: {database_catalog_name}")

database_schema_name = "public"
print(f"database_schema_name: {database_schema_name}")

# COMMAND ----------

# DBTITLE 1,Initialize Workspace Client
def get_single_workspace_client(workspace_url, pat_token):
    try:
        workspace_client = WorkspaceClient(
            host = workspace_url,
            token = pat_token
        )
        return workspace_client
    except Exception as e:
        print(f"ERROR: {e}")

ws_client = get_single_workspace_client(workspace_url, pat_token)

# COMMAND ----------

# DBTITLE 1,Create/Delete OLAP Database
database_uid = create_oltp_db_instance(
    ws_client=ws_client,
    db_instance_name=database_instance_name,
    db_capacity="CU_2"
)

# database_uid = delete_oltp_db_instance(
#     ws_client=ws_client,
#     db_instance_name=database_instance_name,
# )

# COMMAND ----------

# DBTITLE 1,Create/Delete OLAP Database Catalog
create_oltp_db_catalog(ws_client, database_instance_name, database_catalog_name, postgres_database_name)

#delete_oltp_db_catalog(ws_client, database_catalog_name)

# COMMAND ----------

# DBTITLE 1,Get OLAP Database Connection
oltp_db_conn = get_oltp_db_conn(ws_client, database_instance_name, postgres_database_name, user_email)

# COMMAND ----------

# DBTITLE 1,Create an OLTP Table Using Databricks SQL
# Example usage
SQL = """
  CREATE TABLE IF NOT EXISTS products (
    product_id STRING NOT NULL COMMENT 'Unique product identifier',
    product_name STRING COMMENT 'Name of the product',
    category STRING COMMENT 'Product category',
    brand STRING COMMENT 'Brand name',
    price NUMERIC(10,2) COMMENT 'Retail price of the product',
    in_stock BOOLEAN COMMENT 'Is the product currently in stock?',
    release_date DATE COMMENT 'Release date of the product',
    rating DOUBLE COMMENT 'Customer rating (1.0 to 5.0)',
    tags ARRAY<STRING> COMMENT 'List of tags associated with the product',
    specs STRUCT<weight: DOUBLE, color: STRING> COMMENT 'Nested product specifications',
    CONSTRAINT pk_product_id PRIMARY KEY (product_id)
  )
  COMMENT 'Basic product table with sample data types';
"""

# Run conversion
table_name = "products"
table_cols_dict, primary_keys_list = convert_create_table_to_dict(SQL)

delete_oltp_db_table(oltp_db_conn, table_name)
create_oltp_db_table(oltp_db_conn, table_name, table_cols_dict, primary_keys_list)
display_oltp_table_column_datatypes(oltp_db_conn, table_name)

# COMMAND ----------

# DBTITLE 1,Insert Data Using a Dataframe
# Sample product data
data = [
    Row(
        product_id="P001",
        product_name="Smartphone X",
        category="Electronics",
        brand="TechCorp",
        price=699.99,
        in_stock=True,
        release_date=date(2023, 5, 20),
        rating=4.5,
        tags=["5G", "128GB", "OLED"],
        specs={"weight": 0.18, "color": "Black"}
    )
]
# Create DataFrame
df = spark.createDataFrame(data)
display(df)

# Load the dataframe data into the OLTP table
create_oltp_db_table_from_df(
    ws_client = ws_client, 
    db_instance_name = database_instance_name,
    postgres_db_name=postgres_database_name,  
    db_user_name = user_email,
    table_name=table_name,
    df=df,
    mode = "append"
)

display_oltp_table_column_datatypes(oltp_db_conn, table_name)

# COMMAND ----------

# DBTITLE 1,Insert Records Using Postgres SQL

SQL = """
    INSERT INTO products (
        product_id,
        product_name,
        category,
        brand,
        price,
        in_stock,
        release_date,
        rating,
        tags,
        specs
    ) VALUES (
        'P002',
        'Smartphone X',
        'Electronics',
        'TechCorp',
        699.99,
        TRUE,
        DATE '2023-05-20',
        4.5,
        '["5G", "128GB", "OLED"]'::jsonb,
        '{"weight": 0.18, "color": "Black"}'::jsonb
    );
"""

execute_oltp_db_sql(oltp_db_conn, SQL)

# COMMAND ----------

# DBTITLE 1,List All Tables in the OLTP Database Public Schema
tables = list_public_tables(oltp_db_conn)
print(tables)

# COMMAND ----------

# DBTITLE 1,Read All Records From the Table into Spark Dataframe
df = read_oltp_table_into_spark_df(oltp_db_conn, table_name)
display(df) 

# COMMAND ----------

# DBTITLE 1,Query Data From the Table
# Count total records in the table
SQL = f"""
    SELECT count(*) AS record_count FROM {table_name};
"""
display_oltp_db_sql(oltp_db_conn, SQL)
rows, columns = execute_oltp_db_sql(oltp_db_conn, SQL)
df = spark.createDataFrame(rows, columns)
display(df) 


# Show all records in the table
SQL = f"""
    SELECT * FROM {table_name};
"""
display_oltp_db_sql(oltp_db_conn, SQL)
rows, columns = execute_oltp_db_sql(oltp_db_conn, SQL)
df = spark.createDataFrame(rows, columns)
display(df) 


# Expand out JSONB dictionary and list
SQL = f"""
    SELECT
    product_id,
    product_name,
    tags ->> 0 AS tag_1,
    tags ->> 1 AS tag_2,
    tags ->> 2 AS tag_3,
    specs ->> 'color' AS color,
    specs ->> 'weight' AS weight
    FROM {table_name};
"""
display_oltp_db_sql(oltp_db_conn, SQL)

rows, columns = execute_oltp_db_sql(oltp_db_conn, SQL)
df = spark.createDataFrame(rows, columns)
display(df) 