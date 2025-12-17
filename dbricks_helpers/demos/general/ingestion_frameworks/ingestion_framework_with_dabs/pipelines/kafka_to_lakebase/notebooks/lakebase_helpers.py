# Databricks notebook source
# DBTITLE 1,Import General Helpers
# MAGIC %run "./general_helpers"

# COMMAND ----------

# DBTITLE 1,Create New OLTP Database Instance
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

# COMMAND ----------

# DBTITLE 1,Start or Stop OLTP Database
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

# DBTITLE 1,Get OLTP Database Status
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
    import time
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
    import uuid
    try:
        return ws_client.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[db_instance_name]
        ).token
    except Exception as e:
        print(f"❌ ERROR: {e}")

# COMMAND ----------

# DBTITLE 1,Create OLTP Database Catalog
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

# COMMAND ----------

# DBTITLE 1,Create OLTP Database Table From Spark Dataframe
def create_oltp_db_table_from_df(
    ws_client: WorkspaceClient, 
    db_instance_name: str,
    postgres_db_name: str,  
    db_user_name: str,
    table_name: str,
    df,
    mode: str = "append",
):
    """
    Writes a Spark DataFrame to a Databricks oltp PostgreSQL-compatible database instance using JDBC.
    Parameters:
        ws_client (WorkspaceClient): Initialized Databricks SDK workspace client.
        db_instance_name (str): Name of the oltp database instance (must exist and be available).
        postgres_db_name (str): Name of the PostgreSQL database.
        db_user_name (str): Databricks user email used for oltp authentication.
        table_name (str): Target table name to create in the oltp database (e.g., 'public.my_table').
        df (DataFrame): A Spark DataFrame to be written to the oltp table.
        mode (str): Write mode - 'append', 'overwrite', etc.
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


