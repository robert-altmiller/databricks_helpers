# Databricks notebook source
# Replace these variables with your own values
storage_account_name = "st01"
storage_account_key = "<your storage account key>"
blob_container_name = "landingzone"
mount_point = "/mnt/zone_st01"

# Configuration to connect to Azure Blob Storage
conf = {
  "spark.hadoop.fs.azure": "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
  "spark.hadoop.fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": storage_account_key
}

dbutils.fs.mount(
  source = f"wasbs://{blob_container_name}@{storage_account_name}.blob.core.windows.net/",
  mount_point = mount_point,
  extra_configs = {
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
  }
)

print(f"Storage mounted to {mount_point}")
