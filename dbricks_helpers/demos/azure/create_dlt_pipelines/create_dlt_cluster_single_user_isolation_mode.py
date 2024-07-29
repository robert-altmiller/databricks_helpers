# Databricks notebook source
# DBTITLE 1,Include Databricks Helper Functions + Library Imports
# MAGIC %run "../copy_jobs_create_uc_cluster_attach_to_jobs/databricks_helpers"

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
# azure parameters
tenant_id = "da67ef1b-ca59-*************" # IMPORTANT
client_id = "bb08ff7d-7713-42ba-********" # IMPORTANT
client_secret = "******************" # IMPORTANT

# databricks workspace parameters
workspace_url = "https://adb-3809014642669148.8.azuredatabricks.net" # IMPORTANT

# databricks cluster parameters
cluster_name = "my-dlt-cluster" # IMPORTANT
auto_termination_minutes = 120 # IMPORTANT
node_type_id = "Standard_DS3_v2" # IMPORTANT
driver_node_type_id = "Standard_DS3_v2" # IMPORTANT
spark_version = "13.3.x-scala2.12" # IMPORTANT
runtime_engine = "PHOTON"
data_security_mode = "SINGLE_USER"
min_workers = 4 # IMPORTANT
max_workers = 8 # IMPORTANT
spark_env_vars = {
    # "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
    # MOUNT_POINT": "/Volumes/dmp-dev/volumes_datameshvolumes/" # IMPORTANT
}

# libraries to install on databricks cluster
libraries = ["dlt"]

# COMMAND ----------

def get_databricks_token(tenant_id, client_id, client_secret):
    """get databricks token for a service principal"""
    
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" # does not change (Azure Databricks app's Azure AD identifier)
    }

    response = requests.post(token_url, headers=headers, data=payload)
    response_data = json.loads(response.text)

# COMMAND ----------

# DBTITLE 1,DLT Cluster Configuration Settings
# Payload with single user isolation mode and libraries
cluster_payload = {
    "cluster_name": cluster_name,
    "spark_version": spark_version,   # you can specify a specific version if needed
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": node_type_id,
    "driver_node_type_id": driver_node_type_id,
    "spark_env_vars": spark_env_vars,
    "autoscale": {
        "min_workers": min_workers,
        "max_workers": max_workers
    },
    "autotermination_minutes": auto_termination_minutes,
    "enable_elastic_disk": True,
    "enable_local_disk_encryption": False,
    "data_security_mode": data_security_mode,
    "runtime_engine": runtime_engine,
}

# load the cluster config payload
cluster_payload = json.dumps(cluster_payload, indent = 4)
print(cluster_payload)

# COMMAND ----------

# DBTITLE 1,Create Cluster, Load Libraries, Attach to Jobs, Update Job Run As Service Principal
# get a databricks token for a service principal
token = get_databricks_token(tenant_id, client_id, client_secret)

# create unity catalog single user cluster for service principal
if check_cluster_exists(workspace_url, token, cluster_name) == False:
    
    print(f"---start process to create cluster '{cluster_name}'---")
    cluster_id = json.loads(create_databricks_cluster(token, workspace_url, cluster_name, cluster_payload))["cluster_id"]    
    print(f"cluster name '{cluster_name}' cluster id: {cluster_id}")
    
    # monitor cluster creation progress
    get_cluster_status(cluster_id, token, workspace_url)
    
    # install python libraries on cluster
    response = install_libraries_on_databricks(cluster_id, token, workspace_url, libraries)
    print(f"libraries have been installed on cluster {cluster_id}: {response}")
    print(f"---finished process to create cluster '{cluster_name}'---\n")

else: # databricks cluster already exists so get cluster id
    cluster_id = get_databricks_cluster_id(token, workspace_url, cluster_name)
    print(f"cluster '{cluster_name}' already exists\n")

# COMMAND ----------

