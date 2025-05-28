# Databricks notebook source
# DBTITLE 1,Mount Storage Account
container_name = "mycontainername"
storage_account_name = "mystorageaccount"
tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc"
mount_point = f"/mnt/{container_name}"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "XXXXXXX",
    "fs.azure.account.oauth2.client.secret": "XXXXXXX",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Unmount the existing mount point if it exists
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Mount the storage
dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs
)