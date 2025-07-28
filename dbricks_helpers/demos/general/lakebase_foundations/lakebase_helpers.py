# Databricks notebook source
# DBTITLE 1,Import General Helpers
# MAGIC %run "./general_helpers"

# COMMAND ----------

# DBTITLE 1,Create New OLAP Database
def create_oltp_db_instance(
    ws_client: WorkspaceClient, 
    db_instance_name: str, 
    db_capacity: str = "CU_2"
) -> str:
    """
    Creates a new oltp PostgreSQL-compatible database instance in Databricks.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks SDK client.
        db_instance_name (str): Unique name for the oltp database instance.
        db_capacity (str): Capacity class for the instance (e.g., 'CU_2', 'CU_3', 'CU_4').
    Returns:
        str: UID of the created database instance.
    Notes:
        If the instance already exists, the function will print a message and return None.
    """
    # Check if instance already exists
    try:
        existing = ws_client.database.get_database_instance(name=db_instance_name)
        if existing and existing.name == db_instance_name:
            print(f"oltp database instance '{db_instance_name}' already exists.")
            return None
    except Exception:
        # It's okay — the instance doesn't exist, so proceed to create
        pass

    # Create the new database instance
    try:
        db_instance = DatabaseInstance(
            name=db_instance_name,
            capacity=db_capacity
        )
        result = ws_client.database.create_database_instance(db_instance)
        print(f"✅ Created oltp database instance '{db_instance_name}' (UID: {result.uid})")
        return result.uid
    except Exception as e:
        print(f"❌ Failed to create oltp database instance '{db_instance_name}': {e}")

# # Unit test
# database_uid = create_oltp_db_instance(
#     ws_client=ws_client,
#     db_instance_name="cdex",
#     db_capacity="CU_2"
# )

# COMMAND ----------

# DBTITLE 1,Delete OLTP Database Instance
def delete_oltp_db_instance(
    ws_client: WorkspaceClient, 
    db_instance_name: str
):
    """
    Deletes an existing oltp PostgreSQL-compatible database instance in Databricks.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks SDK client.
        db_instance_name (str): Name of the oltp database instance to delete.
    Returns:
        None. Prints the outcome of the delete operation.
    Notes:
        - This performs a hard delete using `purge=True`.
        - If the instance does not exist, an error will be printed.
    """
    try:
        # Check if the instance exists by trying to fetch it
        existing = ws_client.database.get_database_instance(name=db_instance_name)
        # If the instance is found, proceed to delete
        if existing and existing.name == db_instance_name:
            print(f"✅ oltp database instance '{db_instance_name}' exists.")
            result = ws_client.database.delete_database_instance(name=db_instance_name, purge=True)
            print(f"✅ oltp database instance '{db_instance_name}' deleted successfully.")
            return result.uid
    except Exception as e:
        # Print the error if the instance is not found or deletion fails
        print(f"❌ Failed to delete database instance '{db_instance_name}': {e}")


# # Unit test
# delete_oltp_db_instance(ws_client, db_instance_name)

# COMMAND ----------

# DBTITLE 1,Start or Stop OLAP Database
def start_stop_oltp_db(ws_client: WorkspaceClient, db_instance_name: str, start_or_stop_bool: bool = False):
    """
    Starts or stops a Databricks oltp database instance.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks Workspace SDK client.
        db_instance_name (str): Name of the oltp database instance.
        start_or_stop_bool (bool): 
            - False (default): start the instance (stopped=False)
            - True: stop the instance (stopped=True)
    Returns:
        None. Prints the status of the operation.
    """
    try:
        method = "started"
        ws_client.database.update_database_instance(
            name=db_instance_name,
            database_instance=DatabaseInstance(
                name=db_instance_name,
                stopped=start_or_stop_bool
            ),
            update_mask="*"
        )
        if start_or_stop_bool == True:
            method = "stopped"
        print(f"Database instance '{db_instance_name}' {method}.")
    except Exception as e:
        print(f"❌ ERROR: {e}")

# COMMAND ----------

# DBTITLE 1,Get OLAP Database Status
def get_oltp_db_status(ws_client: WorkspaceClient, db_instance_name: str, timeout: int = 1000, poll_interval: int = 5):
    """
    Polls the oltp database instance status until it becomes AVAILABLE.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks Workspace SDK client.
        db_instance_name (str): Name of the oltp database instance.
        timeout (int): Maximum time to wait in seconds (default: 1000).
        poll_interval (int): Time interval between polls in seconds (default: 5).
    Returns:
        None. Prints when the instance becomes AVAILABLE.
    Raises:
        TimeoutError: If the instance does not become AVAILABLE within the timeout.
    """
    start_time = time.time()
    while True:
        instance = ws_client.database.get_database_instance(name=db_instance_name)
        if instance.state == DatabaseInstanceState.AVAILABLE:
            print(f"✅ Instance '{db_instance_name}' is running.")
            return
        if time.time() - start_time > timeout:
            raise TimeoutError(f"⏰ Timeout: Instance '{db_instance_name}' did not start within {timeout} seconds.")
        print(f"⏳ Waiting... Instance '{db_instance_name}' is still stopped.")
        time.sleep(poll_interval)

# COMMAND ----------

# DBTITLE 1,Get OLTP Database Token
def get_oltp_db_token(ws_client: WorkspaceClient, db_instance_name: str) -> str:
    """
    Retrieves a temporary token to connect to the oltp database instance.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks Workspace SDK client.
        db_instance_name (str): Name of the oltp database instance.
    Returns:
        str: A temporary password/token for PostgreSQL connection.
    """
    try:
        return ws_client.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[db_instance_name]
        ).token
    except Exception as e:
        print(f"❌ ERROR: {e}")

# COMMAND ----------

# DBTITLE 1,Get OLTP Database Connection
def get_oltp_db_conn(ws_client: WorkspaceClient, db_instance_name: str, postgres_db_name: str, db_user_name: str):
    """
    Starts the oltp database instance if stopped, waits for availability, and returns a psycopg2 connection.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks Workspace SDK client.
        db_instance_name (str): Name of the oltp database instance.
        db_user_name (str): User email (must match Databricks identity).
    Returns:
        psycopg2.connection: An open connection to the oltp PostgreSQL-compatible database.
    Raises:
        Exception: If connection setup or instance start fails.
    """
    try:
        # Ensure instance is started
        start_stop_oltp_db(ws_client, db_instance_name, False)
        get_oltp_db_status(ws_client, db_instance_name)

        # Get oltp database instance
        instance = ws_client.database.get_database_instance(name=db_instance_name)

        # Return the OLAB database connection
        return psycopg2.connect(
            host=instance.read_write_dns,
            dbname=postgres_db_name,
            user=db_user_name,
            password=get_oltp_db_token(ws_client, db_instance_name),
            sslmode="require"
        )
    except Exception as e:
        print(f"❌ ERROR: {e}")


# # Unit test
# oltp_db_conn = get_oltp_db_conn(ws_client, database_instance_name, postgres_db_name user_email)

# COMMAND ----------

# DBTITLE 1,Create OLAP Database Catalog
def create_oltp_db_catalog(
    ws_client: WorkspaceClient, 
    db_instance_name: str,
    db_catalog_name: str,
    postgres_db_name: str
) -> None:
    """
    Creates a new Unity Catalog catalog for an existing Databricks oltp database instance.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks SDK client.
        db_instance_name (str): Name of the existing oltp database instance.
        db_catalog_name (str): Name of the catalog to create in Unity Catalog.
        postgres_db_name (str): Name of the postgres database to link to the catalog.
    Returns:
        None. Prints status messages indicating success or failure.
    Notes:
        - The catalog will be linked to a schema inside the oltp instance.
        - This version assumes the schema has the same name as the oltp instance.
        - Set `create_database_if_not_exists=True` to auto-create the schema inside the oltp DB if needed.
    """
    try:
        # Ensure instance is started
        start_stop_oltp_db(ws_client, db_instance_name, False)
        get_oltp_db_status(ws_client, db_instance_name)
        
        # Attempt to fetch the oltp instance to verify it exists
        existing = ws_client.database.get_database_instance(name=db_instance_name)
        if existing and existing.name == db_instance_name:
            print(f"✅ oltp database instance '{db_instance_name}' exists. Proceeding to create database catalog '{db_catalog_name}'.")

            # Construct a DatabaseCatalog object
            # Note: We're using the instance name as the internal database name/schema
            database_catalog = DatabaseCatalog(
                name=db_catalog_name,
                database_instance_name=db_instance_name,
                database_name=postgres_db_name,
                create_database_if_not_exists=True
            )

            # Call the API to create the catalog
            ws_client.database.create_database_catalog(database_catalog)
            print(f"✅ Created oltp database catalog '{db_catalog_name}' successfully.")
        else:
            print(f"❌ oltp database instance '{db_instance_name}' does not exist.")
    except Exception as e:
        print(f"❌ Failed to create oltp database catalog '{db_catalog_name}': {e}")


# # Unit test
# create_oltp_db_catalog(ws_client, database_instance_name, database_catalog_name, postgres_db_name)

# COMMAND ----------

# DBTITLE 1,Delete OLAP Database Catalog
def delete_oltp_db_catalog(
    ws_client: WorkspaceClient, 
    db_catalog_name: str
) -> None:
    """
    Deletes a Unity Catalog catalog that is backed by an oltp database instance.

    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks SDK workspace client.
        db_catalog_name (str): Name of the catalog to delete from Unity Catalog.

    Returns:
        None. Prints success or error messages.

    Notes:
        - This operation permanently removes the catalog reference from Unity Catalog.
        - The underlying oltp database instance and schema are not deleted.
    """
    try:
        # Issue the delete command via the Databricks SDK
        ws_client.database.delete_database_catalog(db_catalog_name)
        print(f"✅ Deleted oltp database catalog '{db_catalog_name}' successfully.")
    except Exception as e:
        # Log error if deletion fails
        print(f"❌ Failed to delete oltp database catalog '{db_catalog_name}': {e}")


# # Unit Test
# delete_oltp_db_catalog(ws_client, database_catalog_name)

# COMMAND ----------

# DBTITLE 1,Create OLAP Database Table From Spark Dataframe
def create_oltp_db_table_from_df(
    ws_client: WorkspaceClient, 
    db_instance_name: str,
    postgres_db_name: str,  
    db_user_name: str,
    table_name: str,
    df: pyspark.sql.dataframe,
    mode: str = "overwrite",
):
    """
    Writes a Spark DataFrame to a Databricks oltp PostgreSQL-compatible database instance using JDBC.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks SDK workspace client.
        db_instance_name (str): Name of the oltp database instance (must exist and be available).
        db_user_name (str): Databricks user email used for oltp authentication.
        table_name (str): Target table name to create in the oltp database (e.g., 'public.my_table').
        df (DataFrame): A Spark DataFrame to be written to the oltp table.
    Returns:
        None. Prints success message after the table is written.
    Raises:
        Exception: If database instance lookup, token generation, or JDBC write fails.
    """
    try:
        # Retrieve oltp instance details
        DatabaseInstance = ws_client.database.get_database_instance(name=db_instance_name)

        # Build PostgreSQL JDBC URL
        jdbc_url = (
            f"jdbc:postgresql://instance-{DatabaseInstance.uid}.database.azuredatabricks.net:"
            f"5432/{postgres_db_name}?sslmode=require"
        )

        # Set JDBC connection properties
        connection_properties = {
            "user": db_user_name,
            "password": get_oltp_db_token(ws_client, db_instance_name),
            "driver": "org.postgresql.Driver"
        }

        # Write DataFrame to oltp table
        df = serialize_complex_columns(df)
        df.write \
            .option("stringtype", "unspecified") \
            .jdbc(
                url=jdbc_url,
                table=table_name,
                mode=mode,
                properties=connection_properties
            )
        
        print(f"✅ Successfully {mode} table '{table_name}' in oltp instance '{db_instance_name}' and catalog '{postgres_db_name}'.")
    except Exception as e:
        print(f"❌ ERROR writing DataFrame to oltp DB: {e}")


# # Unit test
# df = spark.createDataFrame([
#     (1, "apple"),
#     (2, "banana"),
#     (3, "cherry")
# ], ["id", "fruit"])

# create_oltp_db_table_from_df(
#     ws_client,
#     database_instance_name,
#     postgres_db_name,
#     db_user_name=user_email,
#     table_name="public.my_fruit_table333",
#     df=df
# )

# COMMAND ----------

# DBTITLE 1,Create OLAP Database Table
def create_oltp_db_table(db_conn, table_name: str, table_columns: dict, primary_keys: list):
    """
    Create a table in the oltp PostgreSQL-connected Databricks database.
    Parameters:
        db_conn (psycopg2.connection): Active oltp DB connection.
        table_name (str): Fully qualified table name (e.g., 'public.my_table').
        table_columns (dict): Dictionary of column names and data types 
                              (e.g., {'id': 'INT', 'name': 'TEXT'}).
    Returns:
        None. Prints success or failure message.
    """
    try:
        # Reset connection state in case a previous transaction failed
        db_conn.rollback()
        # Open a cursor for executing SQL
        cursor = db_conn.cursor()
        
        # Convert the column dictionary into SQL-compatible column definitions
        col_defs = ", ".join([f"{col} {dtype}" for col, dtype in table_columns.items()])
        pkey_defs = ", ".join([f"{key}" for key in primary_keys])

        # Construct the CREATE TABLE SQL statement with primary keys
        create_stmt = f"""CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})"""
        if primary_keys: 
            pkey_defs = ", ".join([f"{key}" for key in primary_keys])
            create_stmt = f"{create_stmt[:-1]}, PRIMARY KEY ({pkey_defs}))"
        print(f"create_stmt: {create_stmt}")
        
        # Execute the CREATE TABLE statement
        cursor.execute(create_stmt)
        # Commit the transaction to persist the table
        db_conn.commit()
        
        # Close the cursor
        cursor.close()
        print(f"✅ Table '{table_name}' created.")
    except Exception as e:
        # Rollback the transaction on error
        db_conn.rollback()
        print(f"❌ Failed to create table '{table_name}': {e}")


# # Unit test
# table_name = "public.my_new_table222222"
# table_cols = {"id": "INT", "fruit": "TEXT"}
# create_oltp_db_table(oltp_db_conn, table_name, table_cols)

# COMMAND ----------

# DBTITLE 1,Delete OLTP Database Table
def delete_oltp_db_table(db_conn, table_name: str):
    """
    Drops a table in the oltp PostgreSQL-connected Databricks database.
    Parameters:
        db_conn (psycopg2.connection): Active oltp DB connection.
        table_name (str): Fully qualified table name (e.g., 'public.my_table').
    Returns:
        None. Prints success or error message.
    """
    try:
        cursor = db_conn.cursor()
        drop_stmt = f"DROP TABLE IF EXISTS {table_name}"
        print(f"drop_stmt: {drop_stmt}")

        cursor.execute(drop_stmt)
        db_conn.commit()
        cursor.close()
        print(f"✅ Table '{table_name}' deleted.")
    except Exception as e:
        db_conn.rollback()
        print(f"❌ Failed to delete table '{table_name}': {e}")


# # Unit test
# table_name = "public.my_new_table222222"
# delete_oltp_db_table(oltp_db_conn, table_name)

# COMMAND ----------

# DBTITLE 1,Execute OLTP SQL Statement
def execute_oltp_db_sql(db_conn, sql_statement = None, insert_update_list = None):
    """
    Execute a SQL statement against the OLTP PostgreSQL-connected Databricks database.
    Parameters:
        db_conn (psycopg2.connection): Active OLTP DB connection.
        sql_statement (str): Raw SQL statement to execute (used if JSON inputs are not provided).
        insert_update_list (list): Contains JSON string to insert or match on in the specified sql_statement.
            - Example with single_json_str: f"INSERT INTO table (column) VALUES (%s::jsonb);, [insert_update_list]"
    Returns:
        None. Prints success or failure message.
    """
    try:
        # Reset connection state in case a previous transaction failed
        db_conn.rollback()

        # Create a new cursor for executing the SQL
        cursor = db_conn.cursor()

        # Track whether this is a write operation
        is_write_op = False

        # If JSON string is provided, insert it into the specified table and column
        if insert_update_list is not None:
            cursor.execute(
                f"{sql_statement}", 
                insert_update_list
            )
            is_write_op = True
        else:
            # Otherwise, execute the provided raw SQL statement
            cursor.execute(sql_statement)
            is_write_op = sql_statement.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP"))

        # Commit only if this was a write operation
        if is_write_op:
            # Commit the transaction
            db_conn.commit()
            print(f"✅ SQL statement executed and committed successfully.")
        else:
            print(f"✅ Read-only SQL statement executed successfully.")
            try:
                rows = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                return rows, colnames
            except psycopg2.ProgrammingError:
                # No results to fetch (e.g., DDL or non-returning statement)
                pass

    except Exception as e:
        # Rollback and report any failure
        db_conn.rollback()
        print(f"❌ Failed to execute SQL statement: {e}")


# Unit test
# execute_oltp_db_sql(oltp_db_conn, sql_statement, single_json_str)

# COMMAND ----------

# DBTITLE 1,Execute OLTP SQL Statement and Show Result
def truncate(val, max_len=80):
    return str(val)[:max_len] + "..." if len(str(val)) > max_len else val


def display_oltp_db_sql(db_conn, sql_statement = None):
    """
    Executes a SQL statement against an OLTP PostgreSQL-connected Databricks database
    and prints all the resulting rows.
    Parameters:
        db_conn (psycopg2.connection): Active OLTP DB connection.
        sql_statement (str): The SQL query to execute.
    Returns:
        None. Prints all the data and columns
    """
    try:
        # Reset connection state in case a previous transaction failed
        db_conn.rollback()
        # Create a new cursor to execute the SQL statement
        cursor = db_conn.cursor()
        # Execute the provided SQL query
        cursor.execute(sql_statement)
        # Fetch all the rows from the result set
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        truncated_rows = [tuple(truncate(cell) for cell in row) for row in rows]

        print(tabulate(truncated_rows, headers=colnames, tablefmt="fancy_grid"))
        
    except Exception as e:
        print(f"❌ Failed to execute SQL statement: {e}")

# Unit test
# display_oltp_db_sql(oltp_db_conn, verify_stmt)

# COMMAND ----------

# DBTITLE 1,Check Column Data Types
def display_oltp_table_column_datatypes(db_conn, table_name):
    """
    Displays the column names, data types, and nullability of a PostgreSQL table.
    Parameters:
        db_conn (psycopg2.connection): Active database connection object.
        table_name (str): Name of the table whose schema information is to be displayed.
    Returns:
        None. The function prints the results to stdout.
    Notes:
        - Queries PostgreSQL system catalogs: pg_attribute, pg_class, pg_namespace, pg_type.
        - Filters for user-defined (non-dropped) columns in the 'public' schema.
        - The SQL query is currently hardcoded for table 'products'; to generalize,
          you should format `table_name` and use query parameterization.
    """
    SQL = """
        SELECT 
            a.attname AS column_name,
            t.typname AS postgres_type,
            a.attnotnull AS is_not_null
        FROM 
            pg_attribute a
        JOIN 
            pg_class c ON a.attrelid = c.oid
        JOIN 
            pg_namespace n ON c.relnamespace = n.oid
        JOIN 
            pg_type t ON a.atttypid = t.oid
        WHERE 
            c.relname = 'products'
            AND n.nspname = 'public'
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY 
            a.attnum;
    """
    display_oltp_db_sql(db_conn, SQL)
    
# COMMAND ----------

# DBTITLE 1,Get Tables Names in an OLTP Database Public Schema
def list_public_tables(db_conn):
    """
    Retrieve all table names in the 'public' schema of a PostgreSQL-compatible OLTP database.
    Parameters:
        oltp_db_conn (psycopg2.connection): An active PostgreSQL database connection.
    Returns:
        List[str]: A list of table names in the 'public' schema.
    Raises:
        Exception: If the query fails or the connection is invalid.
    """
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    except Exception as e:
        raise Exception(f"Failed to list tables in public tables: {e}")


# Unit test
# tables = list_public_tables(oltp_db_conn)
# print(tables)