# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install faker sqlfluff

# COMMAND ----------

# DBTITLE 1,Restart Python Environment
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Python Imports
import json, sqlfluff, time
from pyspark.sql import Row
from faker import Faker
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Fake Data Helper Functions
# MAGIC %run "./fake_data_helpers"

# COMMAND ----------

# DBTITLE 1,Column Masking Helper Functions
# MAGIC %run "./col_masking_helpers"

# COMMAND ----------

# DBTITLE 1,Delta Table Helper Functions
# MAGIC %run "./delta_table_helpers"

# COMMAND ----------

# DBTITLE 1,Local Parameters
# database parameters
catalog = "my_catalog"
schema = "my_schema"
table = "pii_data"
table_fqn = f"{catalog}.{schema}.{table}"

# column mask access parameters
# use 'user_email' or 'group_name' only
email = "altmiller.robert@gmail.com"
group_name = "admins"

# COMMAND ----------

# DBTITLE 1,Insert Data Into PII Data Table
insert_pii_data(table_fqn = table_fqn, records_to_insert = 200)

# COMMAND ----------

# DBTITLE 1,Apply Masking Rules to Table and Show Masked Results
# create masking rule for ssn column
table_fqn, mask_name_fqn = create_col_mask_policy(
    catalog=catalog, schema=schema,
    table_name=table, mask_name="ssn_mask",
    # val is the masking column value
    mask_rule = "CONCAT('XXX-XX-', RIGHT(val, 4))",
    user_email=email, # or group_name=group_name
)
apply_col_mask_policy(table_fqn, mask_name_fqn, col_name="ssn", apply_col_mask=True)


# create column masking rule for email column
table_fqn, mask_name_fqn = create_col_mask_policy(
    catalog=catalog, schema=schema,
    table_name=table, mask_name="email_mask",
    # val is the masking column value
    mask_rule = "CONCAT('***@', SPLIT(val, '@')[1])",
    user_email=email, # or group_name=group_name
)
apply_col_mask_policy(table_fqn, mask_name_fqn, col_name="email", apply_col_mask=True)


# create column masking rule for phone column
table_fqn, mask_name_fqn = create_col_mask_policy(
    catalog=catalog, schema=schema,
    table_name=table, mask_name="phone_mask",
    # val is the masking column value
    mask_rule = "CONCAT('XXX-XXX-', RIGHT(val, 4))",
    user_email=email, # or group_name=group_name
)
apply_col_mask_policy(table_fqn, mask_name_fqn, col_name="phone", apply_col_mask=True)


# display mask results in Delta table
display(spark.sql(f"SELECT * FROM {table_fqn}").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is the point when we try to get the delta table version through time travel
# MAGIC
# MAGIC - Query the existing Delta table column masking functions and reconstruct the SQL for deploying them.
# MAGIC - Remove the existing policies from the Delta table.
# MAGIC - Read Delta table version using time travel feature.
# MAGIC - Recreate the column masking function at the schema level.
# MAGIC - Apply the column masking functions to the Delta table columns.

# COMMAND ----------

# DBTITLE 1,Get the Existing Column Masking Functions
# get existing table mask names and functions
existing_mask_functions = {}
masking_policies = get_column_masking_policies(table_fqn = table_fqn)
for col, mask_policy in masking_policies.items():
    mask_function = reconstruct_masking_function_sql(mask_policy)
    existing_mask_functions[mask_policy.replace("`","")] = [col, mask_function]
print(existing_mask_functions)

# COMMAND ----------

# DBTITLE 1,Remove existing column masking policies from Delta table
# remove existing column masking policies from Delta table
masked_cols_dict = {table_fqn: ["ssn", "email", "phone"]}
remove_col_mask_policy(masked_cols_dict)

# COMMAND ----------

# DBTITLE 1,Check Delta Table Versions
delta_history = show_delta_table_history(table_fqn)

# COMMAND ----------

# DBTITLE 1,Read Time Travel Version and Show DF
# read time travel version into Spark dataframe
delta_version = 8
df_debug = read_delta_table_version(table_fqn, version = delta_version)
display(df_debug.limit(5))

# COMMAND ----------

# DBTITLE 1,Apply Masking Back to Delta Table
for mask_name_fqn, colname_and_mask_function in existing_mask_functions.items():
    spark.sql(colname_and_mask_function[1]) # execute the create mask function sql command
    apply_col_mask_policy(table_fqn, mask_name_fqn, col_name=colname_and_mask_function[0], apply_col_mask=True) # apply the mask function to the column

# COMMAND ----------

# DBTITLE 1,Read Time Travel Version and Try to Show DF (ERROR)
# this should error after reapplying the column masking properties
display(df_debug.limit(5))