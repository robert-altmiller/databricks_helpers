# Databricks notebook source
# DBTITLE 1,Library Imports
import logging, os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# Hive metastore catalog name
catalog = "hive_metastore"

# How to process schemas trigger (User changes this)
schema_processing_method = "all_schemas" # or "all_schemas" or "single_schema"

# How to process schema(s)
if schema_processing_method == "single_schema":
    schemas = ["campaign_audience_builder_dev"]
else: schemas = [db["databaseName"] for db in spark.sql("SHOW DATABASES").collect()]

# Get the mount path storage account locations
mount_paths = {}
for mount in dbutils.fs.mounts():
  mount_paths[mount.mountPoint] = mount.source

# Determine workspace notebook folder path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file_path = f"{notebook_path.rsplit('/', 1)[0]}"

# Min threads for multi-threading
min_threads = 3

# COMMAND ----------

# DBTITLE 1,Initialize Logging
def log_message(log_dir: str, message: str, log_file: str = "write_hive_create_sql.log") -> str:
    """
    Ensures the log directory exists, constructs the log file path, and logs the message
    with a timestamp to both the console and the log file.
    Args:
        log_dir (str): Path to the directory for log files (e.g., "/dbfs/tmp/hive_sql_logs").
        message (str): Message to log.
        log_file (str): Name of the log file (default is "write_hive_create_sql.log").
    Returns:
        str: Full path to the log file.
    """
    # Normalize DBFS URI if needed
    if log_dir.startswith("file:"):
        log_dir = log_dir.replace("file:", "")

    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, log_file)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"{timestamp} [INFO] {message}"

    print(full_message)
    with open(log_file_path, "a") as f:
        f.write(full_message + "\n")

    return log_file_path

# COMMAND ----------

# DBTITLE 1,Find Mount Path Storage Account Locations
def get_mount_point_storage_path(location: str, mounts_dict: str = mount_paths) -> str:
    """
    Resolves a DBFS mount point in the given location path to its underlying storage path.
    Args:
        location (str): The file path containing a DBFS mount point, e.g., 'dbfs:/mnt/my_mount/folder/file.csv'.
        mounts_dict (dict): A dictionary mapping mount points to their underlying storage paths.
                            Example: {"/mnt/my_mount": "abfss://container@account.dfs.core.windows.net"}
    Returns:
        str or None: The resolved storage path with mount point replaced by the actual storage URI.
                     Returns None if no matching mount point is found.
    """
    # Iterate through all known mount points
    for mount_point, mount_source in mounts_dict.items():
        # If the current mount point is found in the location string
        if mount_point in location and "hive/warehouse" not in location:
            # Replace the mount point with its corresponding storage source
            location = location.replace(mount_point, mount_source)
            # Optionally strip the dbfs: prefix if present
            if "dbfs:" in location:
                location = location.replace("dbfs:", "")
            return location  # Return the resolved path
    # If no matching mount source found, return original location
    return location 


def get_storage_path_mount_point(location: str, mounts_dict: dict = mount_paths) -> str:
    """
    Resolves an underlying storage path to its DBFS mount point equivalent.
    This is the inverse of resolving a DBFS mount point to a storage URI.
    Useful when converting real cloud paths (e.g. abfss://...) back to DBFS mount paths.
    Args:
        location (str): The storage path (e.g., 'abfss://...') or potentially a DBFS path.
        mounts_dict (dict): A dictionary mapping DBFS mount points to their underlying storage URIs.
                            Example: {"/mnt/my_mount": "abfss://container@account.dfs.core.windows.net"}
    Returns:
        str: The DBFS-style path using the mount point, e.g., 'dbfs:/mnt/my_mount/folder/file.csv'.
             If no match is found, returns the original input path with 'dbfs:' prefix if not already present.
    """
    # Iterate through each mount point and its storage path
    for mount_point, mount_source in mounts_dict.items():
        # If the storage source is found in the input location
        if mount_source in location and "hive/warehouse" not in location:
            # Replace the storage source URI with the DBFS mount point
            location = location.replace(mount_source, f"dbfs:{mount_point}")
            return location  # Return the mapped DBFS path
    # If no matching mount source found, return original location
    return location

# COMMAND ----------

# DBTITLE 1,Extract Metadata for All Tables in Hive Schema
def get_hive_table_metadata(catalog: str, schema: str) -> list[dict]:
    """
    Extracts metadata for all tables in the specified Hive catalog and schema.
    Args:
        catalog (str): The name of the catalog (e.g., 'hive_metastore').
        schema (str): The name of the schema/database within the catalog.
    Returns:
        list[dict]: A list of metadata dictionaries for each table. Each dictionary includes:
            - catalog (str)
            - schema (str)
            - table (str)
            - location (str or None)
            - provider (str)
            - schema (list[tuple[str, str]]) : List of (column_name, data_type)
            - table_comment (str)
            - column_comments (dict[str, str or None])
    """
    output_folder_path = f"file:/Workspace{workspace_file_path}/catalog={catalog}/schema={schema}"
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    table_names = [row.tableName for row in tables_df.collect()]
    tables_metadata = []
    for table in table_names:
        file_path = f"{output_folder_path.replace('file:', '')}/{table}.sql"
        if not os.path.exists(file_path):
            try:
                desc = spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.{table}").collect()
                metadata = {
                    "catalog": catalog,
                    "schema": schema,
                    "table": table,
                    "location": None,
                    "provider": "parquet",
                    "schema": [],
                    "table_comment": "",
                    "column_comments": {}
                }
                for row in desc:
                    if row.col_name == "Location":
                        metadata["location"] = get_mount_point_storage_path(row.data_type)
                    elif row.col_name == "Provider":
                        metadata["provider"] = row.data_type or "parquet"
                    elif row.col_name == "Comment":
                        metadata["table_comment"] = row.data_type or ""

                # Get column schema and metadata from actual Spark table
                df = spark.table(f"{catalog}.{schema}.{table}")
                for field in df.schema.fields:
                    metadata["schema"].append((field.name, field.dataType.simpleString()))
                    metadata["column_comments"][field.name] = field.metadata.get("comment")
                tables_metadata.append(metadata)
                del df
            except Exception as e:
                message = f"{catalog}.{schema}.{table} ERROR:\n{e}"
                log_message(f"{output_folder_path}/logging", message)
        else: log_message(f"{output_folder_path}/logging", f"path already exists: {file_path}")
    del tables_df

    return tables_metadata

# COMMAND ----------

# DBTITLE 1,Generate Hive Table Create SQL
def generate_hive_create_sql(catalog: str, schema: str, tables_metadata: list[dict]) -> list[dict[str, str]]:
    """
    Generates Hive-compatible CREATE TABLE SQL statements from table metadata.
    Args:
        catalog (str): Catalog name where tables will be created.
        schema (str): Schema name where tables will be created.
        tables_metadata (list[dict]): List of metadata dictionaries from `get_hive_table_metadata`.
    Returns:
        list[dict[str, str]]: A list of dictionaries, each with a single key-value pair:
            - key: Fully qualified table name (underscored)
            - value: Full CREATE TABLE SQL string
    """
    sql_statements = []
    for metadata in tables_metadata:
        hive_table_name = f"{catalog}.{schema}.{metadata['table'].replace('-', '_')}"
        schema_lines = []
        for col, dtype in metadata["schema"]:
            comment = metadata["column_comments"].get(col, "")
            line = f"`{col}` {dtype.upper()}"
            if comment:
                line += f" COMMENT '{comment}'"
            schema_lines.append(line)

        schema_sql = ",\n  ".join(schema_lines)
        location = metadata["location"]
        print(f"location: {location}")
        fmt = metadata["provider"].upper()
        table_comment = f"COMMENT '{metadata['table_comment']}'" if metadata["table_comment"] else ""

        SQL = f"""
CREATE TABLE IF NOT EXISTS {hive_table_name} 
({schema_sql})
USING {fmt}
{table_comment} LOCATION '{location}';
"""
        sql_statements.append({f"{catalog}_{schema}_{hive_table_name}": SQL})
    return sql_statements


# COMMAND ----------

# DBTITLE 1,Write SQL Files to Workspace Filesystem
def write_hive_create_sql(catalog: str, schema: str, sql_statements: list[dict[str, str]]) -> None:
    """
    Writes generated CREATE TABLE SQL statements to files in the Databricks workspace filesystem.
    Args:
        catalog (str): Catalog name (used in output file path).
        schema (str): Schema name (used in output file path).
        sql_statements (list[dict[str, str]]): SQL statements from `generate_hive_create_sql`.
    Returns:
        None
    """
    # Determine workspace notebook folder path
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_file_path = f"{notebook_path.rsplit('/', 1)[0]}"
    output_folder_path = f"file:/Workspace{workspace_file_path}/catalog={catalog}/schema={schema}"

    # Write each SQL file to the workspace filesystem
    for sql_dict in sql_statements:
        for table_fqn, sql in sql_dict.items():
            file_name = f"{table_fqn.split('.')[2]}.sql"
            
            if "hive/warehouse" in sql: # hive metastore only table
                file_name_path = f"{output_folder_path}/source=hive/{file_name}"
                file_name_crc = f"{output_folder_path}/source=hive/.{file_name}.crc"
            else: # external store table
                file_name_path = f"{output_folder_path}/source=external/{file_name}"
                file_name_crc = f"{output_folder_path}/source=external/.{file_name}.crc"

            dbutils.fs.put(file_name_path, sql, overwrite=True)
            dbutils.fs.rm(file_name_crc)  # Clean up checksum files

            message = f"Wrote full SQL script to: {output_folder_path}/{file_name}"
            log_message(f"{output_folder_path}/logging", message)

# COMMAND ----------

# DBTITLE 1,Run Main Program
def process_schema(schema: str) -> str:
    try:
        print(f"✅ Processing schema: {schema}\n")
        tables_metadata = get_hive_table_metadata(catalog, schema)
        sql_statements = generate_hive_create_sql(catalog, schema, tables_metadata)
        write_hive_create_sql(catalog, schema, sql_statements)
        return f"\n✅ Completed schema: {schema}\n"
    except Exception as e:
        output_folder_path = f"file:/Workspace{workspace_file_path}/catalog={catalog}/schema={schema}"
        log_message(f"{output_folder_path}/logging", f"Error processing schema {schema}: {e}")
        return f"❌ Failed schema: {schema}\n"

# Run in multithreaded mode
max_threads = min(min_threads, len(schemas))  # adjust for cluster capability
with ThreadPoolExecutor(max_workers=max_threads) as executor:
    futures = [executor.submit(process_schema, schema) for schema in schemas]
    for future in as_completed(futures):
        print(future.result())

# COMMAND ----------

# DBTITLE 1,Deploy External Tables in UC Workspace Hive Metastore
def read_data(file_path: str) -> str:
    # Read the file contents
    with open(file_path, "r") as f:
        data = f.read()
    return data


def deploy_external_tables(catalog, schemas):
    """
    Deploys external tables by executing SQL files for each table in the given schemas.
    Args:
        catalog (str): The catalog name where the schemas are located.
        schemas (list of str): A list of schema names to process.
    """
    for schema in schemas:
        # Construct the folder path where SQL files are stored for the current schema
        output_folder_path = f"/Workspace{workspace_file_path}/catalog={catalog}/schema={schema}"
        # Retrieve all tables within the current schema
        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        table_names = [row.tableName for row in tables_df.collect()]
        for table in table_names:
            # Construct the path to the SQL file corresponding to the current table
            try:
                file_path = f"{output_folder_path}/source=hive/{table}.sql"
                SQL = read_data(file_path)
            except:
                file_path = f"{output_folder_path}/source=external/{table}.sql"
                SQL = read_data(file_path)
            # Replace any DBFS mount references with their underlying storage paths
            SQL = get_storage_path_mount_point(SQL)
            # Log the SQL being executed (optional for debugging)
            print(SQL)
            # Execute the SQL statement to deploy the external table
            result = spark.sql(SQL)
        del tables_df


# Deploy external tables using the provided catalog and schema list
deploy_external_tables(catalog, schemas)