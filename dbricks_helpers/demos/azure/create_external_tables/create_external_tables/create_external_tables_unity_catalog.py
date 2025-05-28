# Databricks notebook source
# DBTITLE 1,Recursive Iterate Through Storage Account Folder
def list_files(directory_path):
    """list files recursively in storage account folder"""
    file_paths = []
    
    # List items in the current directory
    items = dbutils.fs.ls(directory_path)
    
    for item in items:
        if item.isDir():
            # If it's a directory, recursively process it
            subdirectory_path = item.path
            file_paths.extend(list_files(subdirectory_path))
        else:
            # If it's a file, add its path to the list
            file_paths.append(item.path)
    
    return file_paths

# COMMAND ----------

# DBTITLE 1,Write Finance Dev Domain Sample CSV Folder as External Delta Tables
def create_external_table_adls_csv(
    storage_account_name = None,
    storage_account_key = None,
    container_name = None,
    container_subfolder = None,
    catalog_name = None,
    schema_name = None
):
    """create a Databricks external table from a csv file"""
    # Set the Azure storage account key
    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)
    
    # List files in the specified Azure Data Lake Storage Gen2 container subfolder
    sample_files = list_files(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_subfolder}")
    
    # Filter and print only file paths
    for file in sample_files:
        # UC external table name
        table_name = file.rsplit("/", 1)[0]  # Extract the file name from the path
        table_name = table_name.rsplit("/", 1)[1]
        table_location = file

        print(f"table_name: {table_name}")
        print(f"table_location: {table_location}")

        # SQL statement to create the external table
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

        # Execute the SQL statement
        spark.sql(create_table_sql)
        print(f"External table '{table_name}' created successfully.")

# create_external_table_adls_csv(
#     storage_account_name = "*******",
#     storage_account_key = "*******",
#     container_name = "landingzone",
#     container_subfolder = "General-Accounting/Snowflake-West/sample/",
#     schema_name = 'dmp_frm_dev',
#     database_name = 'sch-frm-********-dev',
#     process_dir_level = False
# )

create_external_table_adls_csv(
    storage_account_name = '*******',
    storage_account_key = '*******',
    container_name = '*******',
    container_subfolder = '/external_tables_test',
    catalog_name = '*******',
    schema_name = 'test2'
)