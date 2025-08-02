# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deep Clone and Table Copy Example From Azure ADLS GEN2 to AWS S3

# COMMAND ----------

# MAGIC %md
# MAGIC # Prerequisite Steps: Mount Storage Accounts to Azure ADLSGen2 and AWS S3

# COMMAND ----------

# DBTITLE 1,Set this on Cluster Spark Settings
# spark.conf.set("spark.databricks.delta.logStore.crossCloud.fatal", False)

# # Example: using azcopy + aws cli
# azcopy copy "https://<storage-account>.dfs.core.windows.net/container/path/*" "./localcopy" --recursive
# aws s3 sync ./localcopy s3://my-bucket/deepclone/baggage --exact-timestamps

# COMMAND ----------

# DBTITLE 1,Mount ADLS Gen 2 Account
# DBTITLE 1,Mount ADLS Gen2 Storage
container_name = "XXXXXeastus"   # <-- replace with your actual container
storage_account_name = XXXXXXeastusstorage"
tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc"
mount_point = f"/mnt/{container_name}"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "dc9c20ce-d8XXXXXXXXXXXX",
    "fs.azure.account.oauth2.client.secret": "jWiXXXXauEBoJpcNsRdXXXXXXXXX",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
}

# Clean existing mount if needed
if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Mount
dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs
)

# COMMAND ----------

# DBTITLE 1,Mount S3 Bucket
# Example: Mount S3 bucket
ACCESS_KEY = "XXXXXXXX"
SECRET_KEY = "XXXXXXX"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cicaktest-rootbucket-f59086ee"
MOUNT_NAME = "cicaktest"

dbutils.fs.mount(
  source = f"s3a://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_BUCKET_NAME}",
  mount_point = f"/mnt/{MOUNT_NAME}"
)

# COMMAND ----------

# DBTITLE 1,Verify What is in S3 Bucket
dbutils.fs.ls("/mnt/cicaktest/deepclone")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create a Delta Cloned Table From Azure to S3 Directly

# COMMAND ----------

# DBTITLE 1,Deep Clone a Table From Azure to S3
# Replace 'source_table' with your table name and adjust the S3 path as needed
spark.sql("""
  CREATE OR REPLACE TABLE delta.`/mnt/cicaktest/deepclone/baggage`
  DEEP CLONE boeingeastus.default.baggage
""")

# COMMAND ----------

# DBTITLE 1,Check Delta Version History on Original AZ Table
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY boeingeastus.default.baggage;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check Delta Version History on Cloned S3 Table
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY delta.`/mnt/cicaktest/deepclone/baggage`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Now Lets Copy the Files Directly From Azure Unity Catalog (UC) Folder Directly to S3

# COMMAND ----------

# DBTITLE 1,Get the Location of the Azure Unity Catalog Table
spark.sql('DESCRIBE DETAIL boeingeastus.default.baggage').collect()[0].location

# COMMAND ----------

# DBTITLE 1,Get Source and Destination Paths and Copy Delta Table Files Directly From UC
# Get the source path using the table name
src_path = f"/mnt/boeingeastus/{spark.sql('DESCRIBE DETAIL boeingeastus.default.baggage').collect()[0].location.split('.net/')[1]}"
print("Source path:", src_path)

s3_table_name = "baggage_files_copied"
dst_path = f"/mnt/cicaktest/deepclone/{s3_table_name}"
print("Destination path:", dst_path)

# Copy files recursively using dbutils
dbutils.fs.cp(src_path, dst_path, recurse=True)

# COMMAND ----------

s3_table_path = "/mnt/cicaktest/deepclone/{s3_table_name}"
spark.sql(f'DESCRIBE DETAIL delta.`{s3_table_path}`').collect()[0].location

# COMMAND ----------

# DBTITLE 1,Check Delta Version History on Copied S3 Table
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY delta.`/mnt/cicaktest/deepclone/baggage_files_copied`;
# MAGIC -- DESCRIBE DETAIL delta.`/mnt/cicaktest/deepclone/baggage_files_copied`;

# COMMAND ----------

# DBTITLE 1,Check For Any References in the Delta Table Logs for Azure Metadata
# Look for any "abfss://" or "dbfs:/" references in the Delta log
logs_path = "/mnt/cicaktest/deepclone/baggage_files_copied/_delta_log"
df_logs = spark.read.text(logs_path)
df_logs.filter(df_logs.value.rlike("abfss://|dbfs:/")).show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Vacuum the Copied S3 Table To Remove Unused Files (Does Not Affect Delta History)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
#spark.sql(f"OPTIMIZE delta.`{s3_delta_table_path}`")
spark.sql(f"VACUUM delta.`{s3_table_path}` RETAIN 0 HOURS")

# COMMAND ----------

# DBTITLE 1,Read a Time Traveled Version of the Copied S3 Table After Vacuum
version = 3  # replace with your desired version number
df_time_travel = spark.read.format("delta").option("versionAsOf", version).load(s3_table_path)
display(df_time_travel)