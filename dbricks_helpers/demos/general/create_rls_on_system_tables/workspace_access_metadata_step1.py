# Databricks notebook source
# DBTITLE 1,Pip Install Packages
# MAGIC %pip install --upgrade databricks-sdk

# COMMAND ----------

# DBTITLE 1,Library Imports
import os, requests, json, time
from base64 import b64encode
import concurrent.futures
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from databricks.sdk.service import iam
from databricks.sdk import AccountClient, WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Local Parameters
try: # Define iam catalog
    catalog = dbutils.widgets.get("iam_catalog")
except:
    catalog = "MY_CATALOG"

try: # Define iam schema
    schema = dbutils.widgets.get("iam_schema")
except:
    schema = "MY_SCHEMA"

# COMMAND ----------

# DBTITLE 1,Get Databricks Token for Databrcks Client Id (AWS)
def get_databricks_token_aws(config):
    token_url = f"https://accounts.cloud.databricks.com/oidc/accounts/{config['account_id']}/v1/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic " + b64encode(f"{config['client_id']}:{config['client_secret']}".encode()).decode()
    }
    payload = {
        "grant_type": "client_credentials",
        "scope": "all-apis"
    }

    response = requests.post(token_url, headers = headers, data = payload)
    response_data = json.loads(response.text)
    if "access_token" in response_data:
        return response_data["access_token"]
    else: print("could not get access token....")

# COMMAND ----------

# DBTITLE 1,Update Secret Scope (Optional)
# try:
#     secret_scope_name = "secret-scope-account"
#     workspace_client.secrets.create_scope(secret_scope_name)
#     print("Created secret scope")
# except Exception as e:
#     print(e)

# try:
#     secret_name = "client_secret"
#     workspace_client.secrets.put_secret(scope=secret_scope_name, key="client_secret", string_value=client_secret)
#     print(f"Created secret {secret_name} in secret scope {secret_scope_name}")
# except Exception as e:
#     print(e)

# COMMAND ----------

# DBTITLE 1,Local Parameters
method = "azure" # aws or azure

def get_config(method = "aws", w=WorkspaceClient()):
    config = {
        "aws":
        {
            # AWS Databricks Account ID
            "account_id": "0d26daa6-5e44-4c97-a497-ef015f91254a",
            "account_host": "https://accounts.cloud.databricks.com",
            # Databricks Service Principal Client ID
            "client_id": "YOUR_CLIENT_ID",
            # Databricks Service Principal Client Secret
            "client_secret": "YOUR_CLIENT_SECRET",
            "workspace_host": "YOUR_WORKSPACE_HOST",
            "workspace_pat": "YOUR_WORKSPACE_TOKEN"
        },  
        "azure": 
        {
            # Azure Tenant ID
            "tenant_id": "YOUR_AZURE_TENANT_ID",
            # Azure Databricks Account ID
            "account_id": "YOUR_ACCOUNT_ID",
            "account_host": "https://accounts.azuredatabricks.net",
            # Azure Service Principal Client ID
            "client_id": "YOUR_CLIENT_ID",
            # Azure Service Principal Client Secret
            "client_secret": "YOUR_CLIENT_SECRET",
            "workspace_host": "YOUR_WORKSPACE_HOST",
            "workspace_pat": "YOUR_WORKSPACE_TOKEN"
        }
    }
    return config[method]


def get_single_workspace_client(method = "aws"):
    config = get_config(method = method)
    try:
        workspace_client = WorkspaceClient(
            host=config["workspace_host"],
            token=config["workspace_pat"]
        )
        return workspace_client
    except Exception as e:
        print(f"ERROR: {e}")


def get_account_client(method = "aws"):
    w = WorkspaceClient()
    config = get_config(method = method, w=w)
    try:
        if method == "aws":
            account_client = AccountClient(
                host=config["account_host"],
                account_id=config["account_id"],
                token = get_databricks_token_aws(config)
            )
        elif method == "azure":
            account_client = AccountClient(
                host=config["account_host"],
                account_id=config["account_id"],
                azure_tenant_id=config["tenant_id"],
                azure_client_id=config["client_id"],
                azure_client_secret=config["client_secret"]
            )
        else: 
            print(f"method {method} is an invalid method")
            return None
        return account_client
    except Exception as e:
        print(f"ERROR: {e}")

# COMMAND ----------

 account_client = get_account_client(method = "azure")

# COMMAND ----------

# DBTITLE 1,Check if Databricks Workspace is Alive
def is_databricks_workspace_alive(url):
    try:
        response = requests.get(url, timeout=5, allow_redirects=True)
        return response.status_code < 500  # 200-499 generally means it's up
    except requests.RequestException:
        return False

# COMMAND ----------

# DBTITLE 1,Get Account Level Workspaces Info
def get_account_workspace_info(method = "aws"):
    account_client = get_account_client(method = method)
    account_workspaces = account_client.workspaces.list()
    acct_ws_data = {}
    ws_count = 0
    for ws in account_workspaces:
        if method == "aws":
            acct_ws_data[f"{ws.workspace_id}"] = [ws.account_id, ws.workspace_name,  f"https://{ws.deployment_name}.cloud.databricks.com"]
        elif method == "azure":
            acct_ws_data[f"{ws.workspace_id}"] = [ws.account_id, ws.workspace_name,  f"https://{ws.deployment_name}.azuredatabricks.net"]
        else: 
            print(f"method {method} is an invalid method")
            return None
        ws_count += 1
    return acct_ws_data, ws_count

acct_ws_data, ws_count = get_account_workspace_info(method = method)
print(f"Found {ws_count} workspaces in {method.upper()} Databricks Account: {get_config(method=method).get('account_id')}")

# COMMAND ----------

# DBTITLE 1,Get Workspace Assignments
def get_workspace_assignments(workspace_id, method = "aws"):
    try: 
        account_client = get_account_client(method = method)
        ws_assignments = account_client.workspace_assignment.list(workspace_id)
        users = admins = []
        for ws_assignment in ws_assignments:
            row =  ws_assignment.as_dict()
            if "USER" in row.get("permissions"):
                users.append(row.get("principal"))
            else: admins.append(row.get("principal"))
        return {"USER": users, "ADMIN": admins}
    except Exception as e:
        print(f"Workspace id '{workspace_id}' ERROR: {e}")
        return {"USER": [], "ADMIN": []}

# COMMAND ----------

# DBTITLE 1,Get Catalog Assignments
def get_service_principal_databricks_id(method = "aws"):
    account_client = get_account_client(method = method)
    client_id = get_config(method = method)["client_id"]
    sp_list = list(account_client.service_principals.list(filter=f"applicationId eq '{client_id}'"))
    return sp_list[0].id


def get_catalog_assignments(workspace_id, workspace_url, method = "aws"):
    try: 
        
        if is_databricks_workspace_alive(workspace_url):
            account_client = get_account_client(method = method)
            
            # Get service principal (sp)
            sp = get_service_principal_databricks_id(method = method)

            # Set SP as workspace admin (do this ONCE)
            account_client.workspace_assignment.update(
                workspace_id=workspace_id,
                principal_id=sp,
                permissions=[iam.WorkspacePermission.ADMIN],
            )

            workspace_obj = account_client.workspaces.get(workspace_id)
            workspace_client = account_client.get_workspace_client(workspace_obj)
            catalogs = workspace_client.catalogs.list()

            catalog_owners_list = []
            for catalog in catalogs:
                catalog_owners_list.append(
                    {   # create a NEW dict in each loop iteration
                        "metastore_id": catalog.metastore_id,
                        "catalog_name": catalog.full_name,
                        "catalog_owner": catalog.owner
                    }                
                )
            print(f"Finished processing workspace: '{workspace_id}', total catalogs: '{len(catalog_owners_list)}'")
            return catalog_owners_list
        else: print(f"skipping workspace '{workspace_url}' with workspace_id '{workspace_id}' because it is not alive")
    except Exception as e:
        print(f"Processing catalogs for Workspace id '{workspace_id}' ERROR: {e}")
        return []

# COMMAND ----------

# DBTITLE 1,Main
json_payload_list = []

def process_workspace(workspace_id, val, method):
    json_payload_dict = {}
    acct_id = val[0]
    ws_name = val[1]
    ws_url = val[2]
    json_payload_dict["account_id"] = acct_id
    json_payload_dict["workspace_id"] = workspace_id
    json_payload_dict["workspace_name"] = ws_name
    json_payload_dict["workspace_url"] = ws_url
    json_payload_dict["permissions"] = get_workspace_assignments(workspace_id, method = method)
    json_payload_dict["catalogs"] = get_catalog_assignments(workspace_id, ws_url, method = method)
    return json_payload_dict


with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for workspace_id, val in acct_ws_data.items():
        futures.append(executor.submit(process_workspace, workspace_id, val, method))
    
    for future in concurrent.futures.as_completed(futures):
        json_payload_list.append(future.result())


# Processing with Json
json_payload = json.dumps(json_payload_list) 

# Processing with Spark
column_names = ["account_id", "workspace_id", "workspace_name", "workspace_url"]
df_workspaces_exploded = spark.read.json(spark.sparkContext.parallelize([json_payload]))
df_workspaces_exploded = df_workspaces_exploded.select(*column_names, "permissions", "catalogs")
df_workspaces_users = df_workspaces_exploded.select(*column_names, F.explode("permissions.USER").alias("users"), "catalogs")
df_workspaces_admins = df_workspaces_exploded.select(*column_names, F.explode("permissions.ADMIN").alias("admins"), "catalogs")

# COMMAND ----------

# DBTITLE 1,Write Results to Delta Table
df_workspaces_exploded.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .saveAsTable("datamesh_sandbox_roadshow_demo_dev_azr_westus.datamesh_iam_schema.all_workspace_and_catalog_perms")

df_workspaces_admins.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .saveAsTable("datamesh_sandbox_roadshow_demo_dev_azr_westus.datamesh_iam_schema.all_workspace_and_catalog_perms_admins")

df_workspaces_users.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .saveAsTable("datamesh_sandbox_roadshow_demo_dev_azr_westus.datamesh_iam_schema.all_workspace_and_catalog_perms_users")