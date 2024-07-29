# Databricks notebook source
# DBTITLE 1,Library Imports
import requests, json, time
from azure.identity import ClientSecretCredential

# COMMAND ----------

# DBTITLE 1,Include Databricks Helper Functions
# MAGIC %run "./databricks_helpers"

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
# azure parameters
tenant_id = "" # IMPORTANT
client_id = "" # IMPORTANT
client_secret = "" # IMPORTANT

# databricks workspace parameters
workspace_url = "" # IMPORTANT

# databricks cluster parameters
cluster_name = "my-databricks-cluster2" # IMPORTANT
auto_termination_minutes = 120 # IMPORTANT
node_type_id = "Standard_DS3_v2" # IMPORTANT
driver_node_type_id = "Standard_DS3_v2" # IMPORTANT
spark_version = "13.3.x-scala2.12" # IMPORTANT
runtime_engine = "PHOTON"
data_security_mode = "SINGLE_USER"
min_workers = 2 # IMPORTANT
max_workers = 8 # IMPORTANT
spark_env_vars = {
    "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
    "MOUNT_POINT": "/Volumes/dmp-dev/volumes_datameshvolumes/" # IMPORTANT
}

# libraries to install on databricks cluster
libraries = ["azure-identity==1.12.0", "azure-storage-file-datalake==12.4.0", "openpyxl==3.1.0", "pandas_profiling==3.6.6", "pydeequ==1.0.1", "pyodbc==4.0.35", "pyyaml", "redis==4.5.1", "sentence_transformers==2.2.2", "yake==0.4.8"]

# databricks job name parameters
job_names_list = [
    "altmiller_volumes_w_Tags_and_classification_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "altmiller_volumes_w_starburst_MMS_SALES",
    "altmiller_volumes_w_Knowledge_Graph_MMS_SALES",
    "altmiller_volumes_w_glossary_json_creation_MMS_SALES",
    "altmiller_volumes_w_glossary_automation_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "altmiller_volumes_w_gen_lineage_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "altmiller_volumes_w_dq_stats_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "altmiller_volumes_w_data_profiling_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "altmiller_volumes_w_cog_search_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "altmiller_volumes_Metrics_and_cache_update"
]

# COMMAND ----------

# DBTITLE 1,Cluster Configuration Settings
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
            
for job_name in job_names_list:
    
    print(f"---start processing job '{job_name}'---\n")
    job_id = get_job_details(token, workspace_url, job_name = job_name)
    print(f"job_name '{job_name}' job id: {job_id}....")
    
    response = attach_cluster_to_job(token, workspace_url, job_id, cluster_id, client_id)
    print(f"cluster '{cluster_name}' attached to job name '{job_name}' response: {response}\n")
    print(f"---finished processing job {job_name}---\n")

