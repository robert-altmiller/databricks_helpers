# Databricks notebook source
# DBTITLE 1,Recursive Iterate Through Storage Account Folder
from py4j.java_gateway import java_import

def list_files_spark_only(directory_path):
    """List files using Hadoop FileSystem, which respects Spark OAuth config"""
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")

    conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.Path(directory_path)
    fs = path.getFileSystem(conf)

    files = []
    for file_status in fs.listStatus(path):
        full_path = file_status.getPath().toString()
        if file_status.isDirectory():
            files.extend(list_files_spark_only(full_path))
        else:
            files.append(full_path)

    return files


# COMMAND ----------

def create_external_table_adls_csv(
    storage_account_name = None,
    container_name = None,
    container_subfolder = None,
    catalog_name = None,
    schema_name = None,
    client_id = None,
    client_secret = None,
    tenant_id = None
):
    """Create a Databricks external table from a CSV file using Azure AD OAuth2 and abfss://"""
    
    # Use abfss endpoint for Gen2 with OAuth
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    for key, value in configs.items():
        spark.conf.set(f"{key}.{storage_account_name}.dfs.core.windows.net", value)
        spark._jsc.hadoopConfiguration().set(f"{key}.{storage_account_name}.dfs.core.windows.net", value)

    base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_subfolder}"

    try:
        sample_files = list_files_spark_only(base_path)  # This function must support abfss paths
    except Exception as e:
        print("Error getting sample data")
        print(f"summary: {e}")
        return

    for file in sample_files:
        table_name = file.rsplit("/", 1)[0].rsplit("/", 1)[1]
        table_location = file

        print(f"table_name: {table_name}")
        print(f"table_location: {table_location}")

        create_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{table_name}`
        USING CSV
        OPTIONS (
          'header' = 'true',
          'inferSchema' = 'true',
          'delimiter' = ',',
          'path' = '{table_location}'
        )
        """
        spark.sql(create_table_sql)
        print(f"External table `{catalog_name}`.`{schema_name}`.`{table_name}` created successfully.")


create_external_table_adls_csv(    
    storage_account_name = 'altstorageadlsgen2',
    container_name = 'bronze',
    container_subfolder = 'example_data',
    catalog_name = 'hive_metastore',
    schema_name = 'default',
    client_id = "XXXXXXXX",
    client_secret = "XXXXXXXX",
    tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc"
)