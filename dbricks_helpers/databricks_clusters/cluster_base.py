# Databricks notebook source
# DBTITLE 1,Get Databricks Rest 2.0 Initial Configuration and Base Functions
# MAGIC %run "../general/base"

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Create Cluster
def create_cluster(dbricks_instance = None, dbricks_pat = None, cluster_name = None, spark_version = None, spark_driver_type = None, username = None):
  """create databricks gpu cluster"""
  jsondata = {
      "autoscale": {
          "min_workers": 4,
          "max_workers": 8,
      },
      "cluster_name": cluster_name,
      "spark_version": spark_version,
      "spark_conf": {},
      "aws_attributes": {
          "first_on_demand": 1,
          "availability": "SPOT_WITH_FALLBACK",
          "zone_id": "auto",
          "spot_bid_price_percent": 100,
          "ebs_volume_count": 0
      },
      "node_type_id": spark_driver_type,
      "driver_node_type_id": spark_driver_type,
      "ssh_public_keys": [],
      "custom_tags": {},
      "spark_env_vars": {},
      "autotermination_minutes": 120,
      "enable_elastic_disk": True,
      "cluster_source": "UI",
      "init_scripts": [],
      "single_user_name": username,
      "enable_local_disk_encryption": False,
      "data_security_mode": "LEGACY_SINGLE_USER",
      "runtime_engine": "STANDARD",
  }
  response = execute_rest_api_call(post_request, get_api_config(databricks_instance, "clusters", "create"), databricks_pat, jsondata)
  return response


# cluster_name = "test-cluster"
# spark_version = "12.2.x-scala2.12"
# spark_driver_type = "Standard_DS3_v2"
# username = spark.sql('select current_user() as user').collect()[0]['user']
# response = create_cluster(databricks_instance, databricks_pat, cluster_name, spark_version, spark_driver_type, username)
# cluster_id = response.text 
# print(f"response: {response}; cluster_id: {cluster_id}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - List All Clusters
def list_clusters(dbricks_instance = None, dbricks_pat = None):
  """get databricks cluster settings"""
  jsondata = None
  response = execute_rest_api_call(get_request, get_api_config(dbricks_instance, "clusters", "list"), dbricks_pat, jsondata)
  return response


# response = list_clusters(databricks_instance, databricks_pat)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Get Cluster Id From Cluster Name
def get_cluster_id(dbricks_instance = None, dbricks_pat = None, cluster_name = None):
  """get a cluster id from a cluster name"""
  response = list_clusters(dbricks_instance, dbricks_pat)
  clusters_json = json.loads(response.text)["clusters"]

  # search for cluster with your name and if it exists return clusterid
  for cluster in clusters_json:
    if cluster["cluster_name"] == cluster_name: # gpu cluster exists
      clusterid = cluster["cluster_id"]
      return clusterid
  return None


# cluster_name = "test-cluster"
# cluster_id = get_cluster_id(databricks_instance, databricks_pat, cluster_name)
# print(f"cluster id: {cluster_id}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Get Cluster Settings
def get_cluster_settings(dbricks_instance = None, dbricks_pat = None, cluster_id = None):
  """get databricks cluster settings"""
  jsondata = {"cluster_id": cluster_id}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, "clusters", "get"), dbricks_pat, jsondata)
  return response


# cluster_id = "0425-161109-onmw1ror"
# response = get_cluster_settings(databricks_instance, databricks_pat, cluster_id)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Start Databricks Cluster
def start_cluster(dbricks_instance = None, dbricks_pat = None, cluster_id = None):
  """start databricks cluster"""
  jsondata = {"cluster_id": cluster_id}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, "clusters", "start"), dbricks_pat, jsondata)
  return response

# cluster_id = "0425-170639-npantus8"
# response = start_cluster(databricks_instance, databricks_pat, cluster_id)
# print(f"cluster id: '{cluster_id}' started; response: {response}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Terminate / Turn Off Cluster
def terminate_cluster(dbricks_instance = None, dbricks_pat = None, cluster_id = None):
  """terminate databricks cluster"""
  jsondata = {"cluster_id": cluster_id}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, "clusters", "delete"), dbricks_pat, jsondata)
  return response


# cluster_id = "0425-161109-onmw1ror"
# response = terminate_cluster(databricks_instance, databricks_pat, cluster_id)
# print(f"cluster id: '{cluster_id}' terminated; response: {response}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Permanently Delete Cluster
def delete_cluster(dbricks_instance = None, dbricks_pat = None, cluster_id = None):
  """permanently delete databricks cluster"""
  jsondata = {"cluster_id": cluster_id}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, "clusters", "permanent-delete"), dbricks_pat, jsondata)
  return response


# cluster_id = "0425-161109-onmw1ror"
# response = delete_cluster(databricks_instance, databricks_pat, cluster_id)
# print(f"cluster id: '{cluster_id}' permanently deleted; response: {response}")
