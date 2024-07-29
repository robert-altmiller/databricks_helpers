# Databricks notebook source
# DBTITLE 1,Library Imports
import os, json
from delta import DeltaTable
from pyspark.sql.functions import expr, col

# COMMAND ----------

# DBTITLE 1,User Parameters (Catalog, Schema, Files Path, etc)
catalog = "dmp_*****"
schema = "test2"
#volumes_files_path = "/Volumes/dmp_frm_dev/volumes_bronze/rebates/YVRBT_NRNA_ACLIN_PEGASUS"
volumes_files_path = "/Volumes/dmp_*****/volumes_lz/archive"
remove_empty_uc_tables = False # or True
update_method = "create_tables" # or "merge_tables"
data_type = "delta" # or "parquet" or "csv"

# COMMAND ----------

# DBTITLE 1,Get a List of All Matched Folders (CSV, Parquet, and Delta Tables)
def directory_file_type(path = None, file_type = None):
    """check for a single file type in a directory"""
    for filename in os.listdir(path.replace("dbfs:", "")):
        if file_type.lower() == "parquet" and filename.lower() == "_delta_log":
            return False
        elif filename.lower().endswith(f".{file_type}"):
            return True
    return False


def is_directory_delta(path = None):
    """check if a directory in ADLS is a Delta table"""
    is_delta_table = 'delta_log' in path and '__tmp_path_dir' not in path
    if is_delta_table: return True
    else: return False


def is_directory_csv(path = None):
    """check if a directory in ADLS is a Delta table"""
    is_csv_dir = 'delta_log' not in path and directory_file_type(path, "csv")
    if is_csv_dir: return True
    else: return False


def is_directory_parquet(path = None):
    """check if a directory in ADLS is a Parquet directory"""
    is_parquet_dir = 'delta_log' not in path and directory_file_type(path, "parquet")
    if is_parquet_dir: return True
    else: return False


def is_directory_of_type(path = None, data_type = None):
    """
    check the type of directory that is being scanned
    types: csv, parquet, delta table formats
    """
    if is_directory_delta(path) and data_type.lower() == "delta":
        # if it's a Delta table folder, append the path one level above
        return True
    elif is_directory_parquet(path) and data_type.lower() == "parquet":
        # if it's a Parquet folder, append the path one level above
        return True
    elif is_directory_csv(path) and data_type.lower() == "csv":
        # if it's a CSV folder, append the path one level above
        return True
    else: return False


def list_volumes_folders(base_path = "/Volumes", recursive = True, data_type = "delta"):
    """lists all the folder in a Databricks Volumes Mount Point"""
    folders = []
    stack = [base_path]

    while stack:
        current_path = stack.pop()
        display_paths = dbutils.fs.ls(current_path)
        for path_info in display_paths:
            path = path_info.path

            if path_info.isDir():
                print(f"path {path} has {data_type} files: {is_directory_of_type(path, data_type)}")
                if is_directory_of_type(path, data_type):
                    if "=" in current_path: 
                        non_partition_path = path.split("=")[0].rsplit("/", 1)[0]
                        folders.append(non_partition_path)
                    elif data_type == "delta": 
                        folders.append(current_path[:-1])  
                    else: folders.append(path[:-1])

                if recursive:
                    stack.append(path)
    return list(set(folders)) # no duplicate paths


# call the function to list folders
folder_paths = list_volumes_folders(base_path = volumes_files_path, recursive = True, data_type = data_type)
print(f"total matched {data_type} folder paths: {len(folder_paths)}")
print(folder_paths)

# COMMAND ----------

# DBTITLE 1,Get Table Partitions (CSV, Parquet, and Delta Tables)
def get_delta_table_partition(delta_table_path = None):
    """get delta table composite partition key in a python list"""
    # read the Delta table and convert it to a DataFrame
    delta_df = DeltaTable.forPath(spark, delta_table_path).history().select("operationParameters")
    # filter rows where "partitionBy" is a key in the map
    filtered_df = delta_df.filter(expr("array_contains(map_keys(operationParameters), 'partitionBy')"))
    # explode the map into individual columns
    exploded_df = filtered_df.selectExpr("operationParameters.partitionBy as partitionBy")
    # get the first value in the first row
    partition_key = exploded_df.first()["partitionBy"]
    return json.loads(partition_key)


def get_table_partition_generic(dir_path = None):
    """
    get directory partition(s) in a python list
    works for parquet and csv partitioned folders
    """
    all_partitions = []
    # Create a stack to keep track of directories to explore
    stack = [dir_path]
    while stack:
        current_dir = stack.pop()
        # list all files and directories in the current directory
        entries = os.listdir(current_dir.replace("dbfs:", ""))
        # separate directories from files
        subdirectories = [entry for entry in entries if os.path.isdir(os.path.join(current_dir, entry))]
        # iterate through subdirectories (potential partitions)
        for subdirectory in subdirectories:
            # construct the full path to the subdirectory
            subdirectory_path = os.path.join(current_dir, subdirectory)
            # check if the subdirectory contains additional partitions
            if '=' in subdirectory:
                # extract the partition key from the directory name
                partition_key = subdirectory.split('=')[0]
                # append the partition key to the list
                all_partitions.append(partition_key)
            # add the subdirectory to the stack for further exploration
            stack.append(subdirectory_path)
    return list(set(all_partitions))


def get_table_partition(dir_path = None, datatype = None):
    """returns a table partition for csv, parquet, and delta"""
    if datatype == "delta": return get_delta_table_partition(dir_path)
    else: return get_table_partition_generic(dir_path)

# COMMAND ----------

# DBTITLE 1,Write Unity Catalog Managed Delta Table
def write_delta_table(df = None, mode = "overwrite", partition_by = None, catalog = None, schema = None, table = None):
    """create a New Unity Catalog Managed Delta Table with or without partition"""
    if len(partition_by) > 0 and partition_by != None:
        df.write \
            .mode(mode) \
            .format("delta") \
            .partitionBy(*partition_by) \
            .saveAsTable(f"{catalog}.{schema}.{table}")
    else:
        df.write \
            .mode(mode) \
            .format("delta") \
            .saveAsTable(f"{catalog}.{schema}.{table}") 

# COMMAND ----------

# DBTITLE 1,Read Source Data (CSV, Parquet, Delta Tables)
def check_csv_df_for_header(csv_dir_path = None, data_type = None):
    """checks a spark dataframe built from csvs if header is needed"""
    # Here, we're checking if the first value in the row is a string.
    print(csv_dir_path)
    df = spark.read.load(csv_dir_path, format = data_type)
    first_row = df.head(1)[0]
    # determine if header should be set to true or false
    header = all(isinstance(value, str) for value in first_row)
    return header


def remove_spaces_from_column_headers(df = None):
    """iterate over column names and remove spaces in spark dataframe"""
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "")  # Remove spaces
        df = df.withColumnRenamed(col_name, new_col_name)
    return df


def read_source_data(dir_path = None, data_type = None):
    """
    read different types of data into spark dataframe
    read csv, parquet, and delta
    """
    if data_type.lower() == "csv" and check_csv_df_for_header(dir_path, data_type) == True:
        return spark.read.load(dir_path, format = data_type, header = True)
    else: return spark.read.load(dir_path, format = data_type)

# COMMAND ----------

# DBTITLE 1,Check If a Unity Catalog Table Exists
def check_table_exists(catalog:str, schema:str, table_name:str):
    query = spark.sql(f"""
            SELECT 1 
            FROM {catalog}.information_schema.tables 
            WHERE table_name = '{table_name}' 
            AND table_schema='{schema}' LIMIT 1""",
        )
    return query.count() > 0

# COMMAND ----------

# DBTITLE 1,Create Databricks Catalog and Schema
try: # create catalog
    spark.sql(f"CREATE CATALOG {catalog}")
except: print(f"could not create catalog {catalog}")

try: # create schema
    spark.sql(f"CREATE SCHEMA {schema}")
except: print(f"could not create schema {schema}\n")

# COMMAND ----------

# DBTITLE 1,Write Delta Tables to Unity Catalog as Managed Delta Tables
if update_method == "create_tables":

    for dir_path in folder_paths:
        # extract the table name from the path
        table_name = dir_path.rsplit("/", 1)[1]
        table_name = table_name.replace("-", "_")

        # check if Unity Catalog (UC) table exists
        table_exists = check_table_exists(catalog, schema, table_name)
        print(f"{catalog}.{schema}.{table_name} table already exists: {table_exists}")
        
        if not table_exists: # then create table

            print(f"creating Unity Catalog managed Delta table for delta table '{catalog}.{schema}.{table_name}'....")
            # read and write delta table to Unity Catalog
            df_source = read_source_data(dir_path.replace("dbfs:", ""), data_type)
            df_source = remove_spaces_from_column_headers(df_source)
            source_partition = get_table_partition(dir_path.replace("dbfs:", ""), data_type)
            print(f"partition for table {table_name} is {source_partition}....")

            write_delta_table(
                df = df_source,
                mode = "overwrite",
                partition_by = source_partition,
                catalog = catalog,
                schema = schema,
                table = f"`{table_name}`"
            )

            print(f"finished creating Unity Catalog managed table '{catalog}.{schema}.{table_name}'....")

            # check if the Delta table has data
            delta_table_row_count = spark.sql(f"""SELECT COUNT(*) FROM {catalog}.{schema}.{table_name}""").first()[0]
            print(f"row count in {catalog}.{schema}.{table_name}: {delta_table_row_count}")
            if delta_table_row_count == 0 and remove_empty_uc_tables == True:
                # check if the Delta table is empty and remove it if it is
                spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")
                print(f"deleted empty unity catalog delta table '{table_name}'")
            print("\n")

# COMMAND ----------

# DBTITLE 1,Keep Unity Catalog Tables in Sync With Storage Account Delta Tables
if update_method == "merge_tables":

    # loop through the Delta tables
    for dir_path in folder_paths:
        # extract the table name from the path
        table_name = dir_path.rsplit("/", 1)[1]
        table_name = table_name.replace("-", "_")
        
        # check if Unity Catalog table exists
        table_exists = check_table_exists(catalog, schema, table_name)
        print(f"{catalog}.{schema}.{table_name} table already exists: {table_exists}")
        
        if table_exists: # then merge table

            # load the target and source Delta tables
            target_table_path = f"{catalog}.{schema}.{table_name}"
            target_df = DeltaTable.forName(spark, target_table_path)
            source_df = read_source_data(dir_path, data_type)
            
            # define the merge condition using all columns as the key
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in source_df.columns])
            
            # perform the merge operation
            # target_df.alias("target").merge(
            #     source_df.alias("source"),
            #     condition=merge_condition
            # ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            print(f"merging Unity Catalog managed Delta table for delta table '{catalog}.{schema}.{table_name}'....")
            # perform the merge operation
            target_df.alias("target").merge(
                source_df.alias("source"),
                condition=merge_condition
            ).whenNotMatchedInsertAll().execute()
            print(f"finished merging Unity Catalog managed Delta table for delta table '{catalog}.{schema}.{table_name}'....\n")

# COMMAND ----------

# DBTITLE 1,Unit Test Remove All Created Unity Catalog Schema Tables
# for delta_path in delta_paths:
#     table_name = delta_path[:-1].rsplit("/", 1)[1]
#     spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Keep Unity Catalog Tables in Sync Example Code

# # Check if Unity Catalog table exists
# table_exists = spark._jsparkSession.catalog().tableExists(f"{audit_catalog}.{audit_schema}.{audit_table}")
# if table_exists: # upsert into the existing audit table
#     print("upserting into existing delta 'audit table'.....")
#     delta_table = DeltaTable.forName(spark, f"{audit_catalog}.{audit_schema}.{audit_table}")
#     delta_table.alias("t").merge(
#         source=df_input_file.alias("s"),
#         condition="""t.catalog_name = s.catalog_name AND 
#                     t.schema_name = s.schema_name AND 
#                     t.table_name = s.table_name AND 
#                     t.column_names = s.column_names"""
#     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#     print("finished upserting into existing 'delta audit' table.....")
# else: # audit table does not exist
#     print("creating delta 'audit table' for the first time....")
#     df_input_file.write.mode("overwrite").format("delta").saveAsTable(f"{audit_catalog}.{audit_schema}.{audit_table}")
#     print("finished creating delta 'audit table' for the first time....")

# COMMAND ----------

# DBTITLE 1,Used for Testing Partitions
# path1 = "dbfs:/mnt/lz_stfrmlanding01/archive/test/test1/test2"
# df1 = spark.read.load(path1, format = "csv", header = True)
# df1.write.mode("overwrite").partitionBy(["name", "mark"]).csv("dbfs:/mnt/lz_stfrmlanding01/archive/test/test1/test4")
# df1.write.parquet("dbfs:/mnt/lz_stfrmlanding01/archive/test/test1/test3", mode="overwrite", partitionBy=["FISC_YR_NUM", "FISC_MTH_NUM"])
# path2 = "/dbfs/mnt/lz_stfrmlanding01/archive/test/test1/test2"
# print(get_table_partition_generic(path2))
