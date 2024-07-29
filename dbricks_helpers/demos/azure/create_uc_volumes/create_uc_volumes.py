# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# DBTITLE 1,Library Imports
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# DBTITLE 1,Storage Account Local Parameters
# storage account connection string information
storage_account_name = ""
storage_account_key = ""
storage_account_conn = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"

# container filters for processing individual container
container_filter = "datameshvolumes"
# container_filter = "idfvolumes" # create a volume for a single container instead of all containers.  Set to 'None' for all containers.

# storage credential name for external location creation
storage_credential_name = "sc-***************"

# catalog name and volumes schemas for IDF Code base
catalog_name = "dmp-***"

# COMMAND ----------

# DBTITLE 1,Get Azure Storage Account Container Names
def get_container_names(storage_connection_string):
    """
    Get a list of container names from an Azure Storage Account.
    Args:
        storage_connection_string (str): The connection string for the Azure Storage Account.
    Returns:
        List[str]: A list of container names.
    """
    container_names = []
    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    # List containers in the storage account
    containers = blob_service_client.list_containers()
    # Extract container names and add them to the list
    for container in containers:
        container_names.append(container['name'])
    return container_names

if container_filter != None: # then process only a single container
    containers = [container_filter]
else: # process all containers in the storage account
    containers = get_container_names(storage_account_conn)
print(f"containers: {containers}")

# COMMAND ----------

# DBTITLE 1,Get Container Root Level Folders
def list_root_folders_in_container(connection_string, container_name):
    """
    List the root folders in a storage account container without scanning every single file.
    Args:
        connection_string (str): The Azure Storage connection string.
        container_name (str): The name of the container in which to list root folders.
    Returns:
        list: A list of unique root folder names in the container.
    """
    try:
        # Create a BlobServiceClient using the provided connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)
        # List blobs in the container
        blobs = container_client.list_blobs()
        # Create a set to store unique root folder names
        root_folders = set()
        # Iterate through the blobs in the container
        for blob in blobs:
            # Split the blob name by '/' to get folder structure
            parts = blob.name.split('/')
            # If there are multiple parts (i.e., it's not just a blob in the root), add the first part as a folder
            if len(parts) == 1: # then a root folder has been found
                print(f"root folder found: {parts[0]}")
                root_folders.add(parts[0])
            else: continue
        # Return the list of root folders as a Python list
        return list(root_folders)
    except Exception as e:
        # Handle exceptions here, e.g., log the error or raise it
        raise e

volumes_dict = {}
for container in containers:
    print(f"getting root level folders for container {container}....")
    folders = list_root_folders_in_container(storage_account_conn, container)
    volumes_dict[container] = folders
    print(f"finished getting root level folders for container {container}....\n")

print(f"volumes_dict: {volumes_dict}")

# COMMAND ----------

# DBTITLE 1,Create Catalog for Volumes
try:
    spark.sql(f"CREATE CATALOG {catalog_name}")
except: 
    print(f"unable to create catalog {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create External Location For Use With Schema Creation
def create_external_location(external_loc_name = None, container = None, storage_account_name = None):
    """this creates an external location using spark sql"""
    SQL = f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{external_loc_name}` URL 'abfss://{container}@{storage_account_name}.dfs.core.windows.net' WITH (STORAGE CREDENTIAL `{storage_credential_name}`)
    """
    spark.sql(SQL)

# COMMAND ----------

# DBTITLE 1,Create Schema With a Managed Location
def create_schema(catalog_name = None, storage_account_name = None, container = None):
    """create a schema with a managed location"""
    SQL = f"""
        CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.volumes_{container} MANAGED LOCATION 'abfss://{container}@{storage_account_name}.dfs.core.windows.net'
    """
    spark.sql(SQL)

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog External Volumes - All Storage Account Containers
for container, rootfolders in volumes_dict.items():
    #create external location if it does not exist
    create_external_location(f"ext-{catalog_name}-{container}", container, storage_account_name)
    
    # create schema for volume if it does not exist
    create_schema(catalog_name, storage_account_name, container)

    volume_location = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net"
    for rootfolder in rootfolders:
        print(f"creating volume for '{container}' container and folder '{rootfolder}'")
        schema = f"volumes_{container}"
        SQL_CREATE = f"""
            CREATE EXTERNAL VOLUME IF NOT EXISTS `{catalog_name}`.`{schema}`.`{rootfolder}`   
            LOCATION '{volume_location}/{rootfolder}';
        """
        spark.sql(SQL_CREATE)
        print(f"created volume for '{container}' container and folder '{rootfolder}'")

# COMMAND ----------


