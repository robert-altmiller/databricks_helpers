# Databricks notebook source
# DBTITLE 1,Library Imports
import json, requests

# COMMAND ----------

# DBTITLE 1,Local Parameters
# base dbfs path
region = "eastus" # or westus
base_dbfs_path = f"/mnt/XXXX{region}"
workspace_name = "<YOUR_WORKSPACE_NAME>"

# write table mappings path
write_table_mappings_path = f"{base_dbfs_path}/workspace={workspace_name}/uuid_table_mappings"
print(f"write_table_mappings_path: {write_table_mappings_path}")

# COMMAND ----------

def make_http_request(method, url, headers=None, params=None, data=None, json_data=None, timeout=30):
    """
    Makes a generic HTTP request.
    :param method: HTTP method (GET, POST, PUT, DELETE, PATCH)
    :param url: Full URL of the API endpoint
    :param headers: Dictionary of request headers
    :param params: Dictionary of query parameters
    :param data: Dictionary or string for form-encoded request body
    :param json_data: Dictionary for JSON request body
    :param timeout: Timeout in seconds (default: 30)
    :return: JSON response or error message
    """
    try:
        # Choose the appropriate request method
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            data=data,
            json=json_data,
            timeout=timeout
        )

        # Raise an error if the request fails
        response.raise_for_status()

        # Try returning JSON response
        return response

    except requests.exceptions.RequestException as e:
        return {"error": "HTTP Request Failed", "message": str(e)}

# COMMAND ----------

# DBTITLE 1,Get Unity Catalog Catalogs and Schemas
def get_unity_catalog_catalogs(databricks_instance, token):
    """Fetches Unity Catalog Catalogs."""
    # API endpoint for listing tables in a specific catalog
    
    url = f"{databricks_instance}/api/2.1/unity-catalog/catalogs"
    # Set up headers with authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Make the API request
    response = make_http_request("GET", url, headers)
    
    # Process response
    catalogs = []
    if response.status_code == 200:
        catalogs_data = response.json().get("catalogs", [])
        for catalog in catalogs_data:
            catalogs.append(catalog["name"])
    return catalogs


def get_unity_catalog_schemas(databricks_instance, token, catalog_name):
    """Fetches Unity Catalog Catalogs."""
    # API endpoint for listing tables in a specific catalog
    
    url = f"{databricks_instance}/api/2.1/unity-catalog/schemas"
    # Query parameters
    params = {
        "catalog_name": catalog_name,
    }
    # Set up headers with authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Make the API request
    response = make_http_request("GET", url, headers, params)
    
    # Process response
    schemas = []
    if response.status_code == 200:
        schemas_data = response.json().get("schemas", [])
        for schema in schemas_data:
            schemas.append(schema["name"])
    return schemas


# Example usage
# DATABRICKS_INSTANCE = "https://adb-4247081124391553.13.azuredatabricks.net"
# TOKEN = ""

# # Fetch and print Unity Catalog tables
# catalogs = get_unity_catalog_catalogs(DATABRICKS_INSTANCE, TOKEN)
# print(catalogs)
# schemas = get_unity_catalog_schemas(DATABRICKS_INSTANCE, TOKEN, "boeingeastus")
# print(schemas)

# COMMAND ----------

# DBTITLE 1,Get Unity Catalog Tables

def get_unity_catalog_tables(databricks_instance, token, catalog_name, schema_name):
    """Fetches table metadata from Databricks Unity Catalog."""
    # API endpoint for listing tables in a specific catalog
    
    url = f"{databricks_instance}/api/2.1/unity-catalog/tables"
    # Query parameters
    params = {
        "catalog_name": catalog_name,
        "schema_name": schema_name
    }
    # Set up headers with authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Make the API request
    response = make_http_request("GET", url, headers, params)

    # Process response
    if response.status_code == 200:
        tables_data = response.json().get("tables", [])
        table_mapping_dict = {}

        for table in tables_data:
            uc_tablename_fqn = table["full_name"]
            try: 
                metastore_id = table["metastore_id"]
            except: metastore_id = None
            try: 
                table_id = table["table_id"]
            except: table_id = None
            table_metadata = {
                "uc_metastore_id": metastore_id,
                "uc_catalog_name": table["catalog_name"],
                "uc_schema_name": table["schema_name"],
                "uc_table_uuid": table_id,
                "uc_table_type": table["table_type"],
                #"uc_table_format": table["data_source_format"],
                "uc_table_name": table["name"],
                "uc_tablename_fqn": uc_tablename_fqn,
                "uc_storage_acct_location": table.get("storage_location", "N/A"),  # Handle missing keys
                "uc_table_owner": table.get("owner", "N/A"),
            }
            table_mapping_dict[uc_tablename_fqn] = table_metadata
        return table_mapping_dict
    else:
        return json.dumps({"error": response.status_code, "message": response.text}, indent=4)

# # Example usage
# DATABRICKS_INSTANCE = "https://adb-4247081124391553.13.azuredatabricks.net"
# TOKEN = ""
# CATALOG_NAME = "boeingeastus"
# SCHEMA_NAME = "default"

# # Fetch and print Unity Catalog tables
# table_metadata_dict = get_unity_catalog_tables(DATABRICKS_INSTANCE, TOKEN, CATALOG_NAME, SCHEMA_NAME)
# print(table_metadata_dict)

# COMMAND ----------

# DBTITLE 1,MAIN: Create UUID Table Mappings
DATABRICKS_INSTANCE = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

uuid_table_map_dict = {}
uuid_table_map_list = []
catalogs = get_unity_catalog_catalogs(DATABRICKS_INSTANCE, TOKEN)
for catalog in catalogs:
    schemas = get_unity_catalog_schemas(DATABRICKS_INSTANCE, TOKEN, catalog)
    for schema in schemas:
        print(f"Processing catalog: {catalog}, schema: {schema}")
        tables_dict = get_unity_catalog_tables(DATABRICKS_INSTANCE, TOKEN, catalog, schema) 
        uuid_table_map_dict.update(tables_dict)
uuid_table_map_list.append(uuid_table_map_dict)
uuid_table_map_list_json = json.dumps(uuid_table_map_list, indent=4)

# COMMAND ----------

# DBTITLE 1,Write Results to Mounted Storage Account
# Write JSON to DBFS
dbutils.fs.put(write_table_mappings_path, uuid_table_map_list_json, overwrite=True)
print(f"JSON table mappings file successfully written to: {write_table_mappings_path}")