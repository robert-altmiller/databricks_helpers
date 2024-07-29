# Databricks notebook source
# DBTITLE 1,Workflow Parameters / Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Install Libraries
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Library Imports
import os, time
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import LongType
from pyspark.sql import Window
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Set Databricks Widgets
time.sleep(5)

dbutils.widgets.text("volumes_path", "/Volumes/dmp_frm_dev/volumes_datamasking/col_masking_input", "volumes_path")
volumes_path = dbutils.widgets.get("volumes_path")
print(f"volumes_path: {volumes_path}")

dbutils.widgets.text("audit_catalog", "dmp_frm_dev", "audit_catalog")
audit_catalog = dbutils.widgets.get("audit_catalog")
print(f"audit_catalog: {audit_catalog}")

dbutils.widgets.text("audit_schema", "metadata", "audit_schema")
audit_schema = dbutils.widgets.get("audit_schema")
print(f"audit_schema: {audit_schema}")

dbutils.widgets.text("audit_table", "audit_rules", "audit_table")
audit_table = dbutils.widgets.get("audit_table")
print(f"audit_table: {audit_table}")

dbutils.widgets.text("drop_audit_table", "False", "drop_audit_table")
drop_audit_table = dbutils.widgets.get("drop_audit_table")
print(f"drop_audit_table: {drop_audit_table}")

# COMMAND ----------

# DBTITLE 1,Local Audit Table Parameters (User Defined)
# Define the catalog, schema and table name for the audit metadata table where all the rules will be stored
# audit_catalog = "dmp_frm_dev"
# audit_schema = "metadata"
# audit_table = "audit_rules"

# COMMAND ----------

# DBTITLE 1,Import Masking Data from Excel CSV or XLSX and Create Masking Metadata Delta Table
# get the current notebook's path
# notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# get the parent directory of the notebook's location
# parent_dir_dbfs = os.path.dirname(notebook_path)

# create path for xlsx masking input parameters
input_base_path = volumes_path
input_filename = "masking.xlsx" # or masking.csv
input_file_path = f"{input_base_path}/{input_filename}"

# read masking input file into spark df
df_input_file = spark.createDataFrame(pd.read_excel(input_file_path))

# add_last_updated column to 'df_input_file' dataframe
df_input_file = df_input_file \
  .withColumn("id", F.col("id").cast(LongType())) \
  .withColumn("last_updated", F.current_timestamp())

# check to drop the audit table
if drop_audit_table == "True":
    print("dropping the 'audit table'....")
    spark.sql(f"DROP TABLE {audit_catalog}.{audit_schema}.{audit_table}")

# Check if Unity Catalog table exists
table_exists = spark._jsparkSession.catalog().tableExists(f"{audit_catalog}.{audit_schema}.{audit_table}")
if table_exists: # upsert into the existing audit table
    print("upserting into existing delta 'audit table'.....")
    delta_table = DeltaTable.forName(spark, f"{audit_catalog}.{audit_schema}.{audit_table}")
    delta_table.alias("t").merge(
        source=df_input_file.alias("s"),
        condition="""t.catalog_name = s.catalog_name AND 
                    t.schema_name = s.schema_name AND 
                    t.table_name = s.table_name AND 
                    t.column_names = s.column_names"""
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("finished upserting into existing 'delta audit' table.....")
else: # audit table does not exist
    print("creating delta 'audit table' for the first time....")
    df_input_file.write.mode("overwrite").format("delta").saveAsTable(f"{audit_catalog}.{audit_schema}.{audit_table}")
    print("finished creating delta 'audit table' for the first time....")

# check masking input filename as a table in UC
SQL = f"""
  SELECT * FROM `{audit_catalog}`.`{audit_schema}`.`{audit_table}`
"""
audit_df = spark.sql(SQL)
display(audit_df)

# COMMAND ----------

audit_df.printSchema()
