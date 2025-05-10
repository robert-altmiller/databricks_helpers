# Databricks notebook source
# DBTITLE 1,Library Imports
import json, requests
from datetime import datetime
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Local Parameters
# User email address
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(f"user_email: {user_email}")

# Notebook parent path
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
nb_parent_path = '/'.join(nb_path.strip('/').split('/')[:-1])
print(f"nb_parent_path: {nb_parent_path}")

# Local path for writing data
json_data_path = f"/Workspace/{nb_parent_path}/unity_catalog.json"  
print(f"json_data_path: {json_data_path}")

# Local path for logging
log_path = f"/Workspace/{nb_parent_path}/unity_catalog_runlog.json"  
print(f"log_path: {log_path}")

# Databricks workspace url
databricks_instance = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
print(f"databricks_instance: {databricks_instance}")

# Databricks pat token
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
print(f"databricks_token: {databricks_token}")

# COMMAND ----------

# DBTITLE 1,Unity Catalog Inspector Class
class UnityCatalogInspector:
    """
    A utility class to extract and inspect catalogs, schemas, and tables metadata from Unity Catalog in a Databricks workspace.
    """

    def __init__(self, spark: SparkSession, databricks_instance: str, databricks_token: str, write_path: str,  log_path: str, overwrite_log: bool = False):
        """
        Initializes the UnityCatalogInspector.
        Args:
            spark (SparkSession): Active Spark session.
            databricks_instance (str): Databricks workspace URL (e.g. 'https://adb-xxxx.azuredatabricks.net').
            databricks_token (str): Databricks personal access token (PAT).
            write_path (str): Optional path for future export of collected metadata.
        """
        self.spark = spark
        self.databricks_instance = databricks_instance.rstrip("/")
        self.token = databricks_token
        self.write_path = write_path
        self.log_path = log_path
        self.overwrite_log = overwrite_log
        self._clear_log()
        self._log("🔧 Initializing Unity Catalog Inspector...")
        self._log(f"📍 Workspace URL set to: {self.databricks_instance}")

    def _clear_log(self):
        """
        Clears the current log file if logging is enabled.
        Behavior:
            - Deletes the contents of the log file.
            - Only runs if self.log_path is set.
        """
        if hasattr(self, "log_path") and self.log_path:
            with open(self.log_path, "w+") as f:
                f.write("")  # Overwrite with empty content
            print(f"🧹 Cleared existing log: {self.log_path}")


    def _log(self, message: str):
        """
        Logs a message to the notebook output and optionally to a log file, with timestamp.
        Args:
            message (str): Message to log.
            overwrite (bool): If True, replaces the log file. Otherwise, appends. Default is False.
        Behavior:
            - Adds a timestamp to each line.
            - Always prints to notebook.
            - Writes to log file if self.log_path is set.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] --> {message}"
        print(formatted)  # Always print to notebook
        if hasattr(self, "log_path") and self.log_path:
            with open(self.log_path, "a") as f:
                f.write(formatted + "\n")

    def _make_http_request(self, method, url, headers=None, params=None, data=None, json_data=None, timeout=30):
        """
        Internal helper to issue HTTP requests to the Databricks REST API.
        Returns:
            response object (requests.Response) or dict with error details.
        """
        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                data=data,
                json=json_data,
                timeout=timeout
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            return {"error": "HTTP Request Failed", "message": str(e)}

    def _auth_headers(self):
        """
        Constructs standard authorization headers for Databricks API calls.
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get_catalogs(self):
        """
        Lists all catalogs in Unity Catalog.
        Returns:
            List[str]: Catalog names.
        """
        self._log("📦 Fetching catalogs...")
        url = f"{self.databricks_instance}/api/2.1/unity-catalog/catalogs"
        response = self._make_http_request("GET", url, headers=self._auth_headers())
        if isinstance(response, dict):
            self._log("⚠️ Failed to retrieve catalogs.")
            return []
        catalogs = [catalog["name"] for catalog in response.json().get("catalogs", [])]
        self._log(f"✅ Found {len(catalogs)} catalogs.")
        return catalogs

    def get_schemas(self, catalog_name):
        """
        Lists schemas for a given catalog.
        Args:
            catalog_name (str): Name of the catalog.
        Returns:
            List[str]: Schema names within the catalog.
        """
        self._log(f"📂 Fetching schemas for catalog '{catalog_name}'...")
        url = f"{self.databricks_instance}/api/2.1/unity-catalog/schemas"
        params = {"catalog_name": catalog_name}
        response = self._make_http_request("GET", url, headers=self._auth_headers(), params=params)
        if isinstance(response, dict):
            self._log(f"⚠️ Failed to retrieve schemas for catalog: {catalog_name}")
            return []
        schemas = [schema["name"] for schema in response.json().get("schemas", [])]
        self._log(f"  ➕ Found {len(schemas)} schemas in catalog '{catalog_name}'")
        return schemas

    def get_tables(self, catalog_name, schema_name):
        """
        Retrieves metadata for all tables in a specified catalog and schema.
        Args:
            catalog_name (str): Name of the catalog.
            schema_name (str): Name of the schema.
        Returns:
            Dict[str, dict]: Mapping of table FQN to metadata.
        """
        self._log(f"    📄 Fetching tables for {catalog_name}.{schema_name}...")
        url = f"{self.databricks_instance}/api/2.1/unity-catalog/tables"
        params = {"catalog_name": catalog_name, "schema_name": schema_name}
        response = self._make_http_request("GET", url, headers=self._auth_headers(), params=params)
        if isinstance(response, dict):
            self._log(f"    ⚠️ Failed to fetch tables for {catalog_name}.{schema_name}")
            return {}

        table_mapping_dict = {}
        for table in response.json().get("tables", []):
            fqn = table["full_name"]
            table_mapping_dict[fqn] = {
                "uc_metastore_id": table.get("metastore_id", "N/A"),
                "uc_catalog_name": table.get("catalog_name", catalog_name),
                "uc_schema_name": table.get("schema_name", schema_name),
                "uc_table_uuid": table.get("table_id", "N/A"),
                "uc_table_type": table.get("table_type", "N/A"),
                "uc_table_name": table.get("name", "N/A"),
                "uc_tablename_fqn": fqn,
                "uc_storage_acct_location": table.get("storage_location", "N/A"),
                "uc_table_owner": table.get("owner", "N/A"),
            }
        self._log(f"    ✅ Retrieved {len(table_mapping_dict)} tables.")
        return table_mapping_dict

    def collect_table_mappings(self):
        """
        Collects metadata across all catalogs and schemas.
        Returns:
            Dict[str, dict]: Mapping of table FQN to metadata.
        """
        self._log("🚀 Starting metadata collection...")
        all_table_mappings = {}
        for catalog in self.get_catalogs():
            for schema in self.get_schemas(catalog):
                tables = self.get_tables(catalog, schema)
                all_table_mappings.update(tables)
        self._log(f"🏁 Completed metadata collection. Total tables: {len(all_table_mappings)}")
        return all_table_mappings

    def get_table_mappings(self, return_type: str = "json"):
        """
        Returns collected table metadata in the desired format.
        Args:
            return_type (str): 'json' or 'spark'
        Returns:
            str or DataFrame: JSON string or Spark DataFrame with metadata.
        """
        self._log(f"📤 Returning metadata as {return_type.upper()}...")
        table_dict = self.collect_table_mappings()

        if return_type == "spark":
            rows = list(table_dict.values())
            return self.spark.createDataFrame(rows)
        elif return_type == "json":
            return [table_dict]
        else:
            raise ValueError("return_type must be 'json' or 'spark'")


# COMMAND ----------

# DBTITLE 1,Run Main Program
return_type = "json" # or spark

# Initialize the inspector
inspector = UnityCatalogInspector(spark, databricks_instance, databricks_token, json_data_path, log_path, overwrite_log = True)

# Run and return results
if return_type == "spark":
    df = inspector.get_table_mappings(return_type="spark")
    display(df)
else:
    json_result = inspector.get_table_mappings(return_type="json")
    
    # Write the results
    with open(json_data_path, "w+") as f:
        json.dump(json_result, f)
    print(f"✅ Metadata written to: {json_data_path}")

    # Print the results
    with open(json_data_path, "r") as f:
        metadata = json.load(f)
    print(json.dumps(metadata, indent=2))