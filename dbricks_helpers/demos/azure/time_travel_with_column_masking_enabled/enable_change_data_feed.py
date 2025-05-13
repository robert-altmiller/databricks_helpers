# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install faker

# COMMAND ----------

# DBTITLE 1,Restart Python Environment
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Python Imports
from faker import Faker
from pyspark.sql import Row
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Delta Table Helper Functions
# MAGIC %run "./delta_table_helpers"

# COMMAND ----------

# MAGIC %run "./fake_data_helpers"

# COMMAND ----------

# DBTITLE 1,Local Parameters
# database parameters
catalog = "starbuckseastus"
schema = "main"
table = "pii_data"
table_fqn = f"{catalog}.{schema}.{table}"

# COMMAND ----------

# DBTITLE 1,Insert Data Into PII Data Table
insert_pii_data(table_fqn = table_fqn, records_to_insert = 500)

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed on Delta Table
# Create a new Delta table version with CDF enabled
spark.sql(f"ALTER TABLE {table_fqn} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# DBTITLE 1,Get Delta Table History Latest Version and First CDF Version
delta_history = show_delta_table_history(table_fqn)

# Get the first Delta history row record
delta_history_first_row = delta_history.collect()[0]  # Row index 0 = first row (most recent version)

# Get the latest Delta table version
latest_delta_version = delta_history_first_row["version"]
print(f"latest_delta_version: {latest_delta_version}")

# Find the first Delta version where CDF was enabled
first_cdf_enable_version = (
    delta_history
    .filter("operation = 'SET TBLPROPERTIES'")
    .filter("operationParameters.properties LIKE '%delta.enableChangeDataFeed%true%'")
    .orderBy("version")
    .limit(1)
    .collect()[0]["version"]
)
print(f"first_cdf_enable_version: {first_cdf_enable_version}")

# COMMAND ----------

# DBTITLE 1,Add a New Column to Delta Table
spark.sql(f"""
    ALTER TABLE {table_fqn}
    ADD COLUMNS (new_column_added STRING COMMENT 'new_column_added');
""")

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
delta_history = show_delta_table_history(table_fqn)

# COMMAND ----------

# DBTITLE 1,Update the New Column in Delta Table
spark.sql(f"""
    UPDATE {table_fqn}
    SET new_column_added = 'na';
""")

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
delta_history = show_delta_table_history(table_fqn)

# COMMAND ----------

# DBTITLE 1,Insert Data into PII Data Table
insert_pii_data(table_fqn = table_fqn, records_to_insert = 500)

# COMMAND ----------

# DBTITLE 1,Check Delta Table History
delta_history = show_delta_table_history(table_fqn)

# COMMAND ----------

# DBTITLE 1,Get a  CDF Snapshop From a Group of Versions


# The 'cdf_version_end' needs to be greater than the 'cdf_version_start' and <= 'latest_delta_version'
cdf_version_start = 2 #first_cdf_enable_version
cdf_version_end = 5 #latest_delta_version

df = spark.sql(f"""
        SELECT {', '.join([col for col in spark.table(table_fqn).columns])}, _change_type
        FROM table_changes('{table_fqn}', {cdf_version_start}, {cdf_version_end})
        WHERE _change_type != 'update_preimage'
""")
df = df.dropDuplicates()
display(df.orderBy(F.col("first_name").asc(), F.col("last_name").asc()))

# COMMAND ----------

# DBTITLE 1,Read Delta Table Directly Without CDF
display(spark.sql("SELECT * FROM starbuckseastus.main.pii_data").orderBy(F.col("first_name").asc(), F.col("last_name").asc()))