# Databricks notebook source
# MAGIC %md
# MAGIC ### Problem Statement: 
# MAGIC UCX tool is only useful for non UC to UC cloud migrations.  As customers need to migrate over to different cloud environments, and are aleady on UC, they may need to engage PS or size their environment to estimate migration effort.  Typically the only other way is to leverage the TF exporter.  This is helpful but can sometimes take over 24 hours in large workspaces.   While the TF exporter will have far more detail and be more comprehensive, this script opts for speed over granularity. The goal of this script is to essentially cut this time down by leveraging DB APIs instead and is useful when you just want a ballpark estimate of key elements such as how many clusters, notebooks, jobs, etc.
# MAGIC
# MAGIC ###Goal: 
# MAGIC To be able to count the number of workspace objects and get a count of what exists within a system for migration.  Our PS typically needs the following information to scope a UC to UC cloud migration engagement.
# MAGIC
# MAGIC https://docs.google.com/spreadsheets/d/1e2dwF3XMwPx_M4xOko6br4_WV_ueLu0rp63soBOUTP0/edit?gid=143005345#gid=143005345
# MAGIC
# MAGIC ###Note: 
# MAGIC
# MAGIC For Databricks - Before sharing externally, please remove my PAT token and have your customer regenerate their own before they run.  Also, please delete every block under the section ARCHIVED.
# MAGIC
# MAGIC PreFlight Steps:  Please be sure to have a PAT token and have your host.  
# MAGIC
# MAGIC **LIMITATION 1:** This will be capped based on the user so user must have the appropriate access or they will not capture everything.
# MAGIC
# MAGIC **LIMITATION 2:** There are API limits as defined by this doc https://docs.databricks.com/aws/en/resources/limits
# MAGIC
# MAGIC ###Sample Output
# MAGIC You will see the relevant key, the URL response, and a status.  400 is a bad request, 404 is not found, 200 is good.  The standard responses.  
# MAGIC ![Screenshot 2025-07-29 at 10.30.21 AM.png](./Screenshot 2025-07-29 at 10.30.21 AM.png "Screenshot 2025-07-29 at 10.30.21 AM.png")
# MAGIC
# MAGIC **Original Author:** Peter Lam (p.lam@databricks.com)
# MAGIC
# MAGIC **Special thanks to:** _Breanna Barton_ - talking me through some of the Terraform Script and the pros and cons which emboldened me to test alternatives.  _Jason Scalia_ for reminding me that more specific calls to APIs exist this inspired the async approach which gets this information faster.  Jason also provided the CLI equivalent for the SCIM ones which conceptually is the same but cannot be replicated on the Databricks side. _Danilo De Oliveria Perez_ - code review.  Thanks to _Dipankar Kushari_ to include ways to gather non UC information without APIs via the UCX code base.
# MAGIC

# COMMAND ----------

import asyncio
%pip install aiohttp
import aiohttp
import os

db_instance = f"https://{str(spark.conf.get('spark.databricks.workspaceUrl'))}"
db_token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
header = {"Authorization": f"Bearer {db_token}"}

#All the API Endpoints from: https://docs.databricks.com/api/workspace/introduction - feel free to examine what you need here.
api_endpoints = {
    #Compute
    'clusters':'/api/2.0/clusters/list',
    'cluster_policies':'/api/2.0/policies/clusters/list',
    'instance_pools':'/api/2.0/instance-pools/list',
    #Notebooks
    'notebooks':'/api/2.0/workspace/list?path=/',
    'databricks_repos': '/api/2.0/repos',
    #Metastore - note - this needs more detail so won't be in the main async call
    'uc_metastore_tables':'	/api/2.1/unity-catalog/tables',
    #JOBS
    'jobs':'/api/2.2/jobs/list',
    'dlt_pipelines':'/api/2.0/pipelines',
    #MLFlow
    'mlflow_models':'/api/2.0/mlflow/registered-models/list',
    "mlflow_experiments": "/api/2.0/mlflow/experiments/list", 
    #Workspace
    'workspace_root':'/api/2.0/workspace/list?path=/',
    'workspace_conf': '/api/2.0/workspace-conf',
    'global_init_scripts': '/api/2.0/global-init-scripts',
    # DB SQL - widget and visualization does not exist
    'dbsql_endpoint':'/api/2.0/sql/warehouses',
    'dbsql_global_config':'/api/2.0/global-init-scripts',
    #This is only the post command for SQL statements - it looks like the get requires the entire statement_id.  This can't be enumerated /api/2.0/sql/statements/{statement_id}
    # 'dbsql_query':'/api/2.0/sql/statements',
    'dbsql_dashboard':'/api/2.0/lakeview/dashboards',
    #/api/2.0/permissions/queries/ and /api/2.0/permissions/dashboards/ is deprecated and replaced with: /api/2.0/permissions/{workspace_object_type}/{workspace_object_id}.  Can't get these
    # 'dbsql_permissions_endpoint':'/api/2.0/permissions/dashboards',
    # 'dbsql_permissions_query':'/api/2.0/permissions/queries',
    'dbsql_alert':'/api/2.0/sql/alerts',
}

# COMMAND ----------

import nest_asyncio
import aiohttp
import asyncio

nest_asyncio.apply()

async def fetch_count(session, key, url):
    try:
        async with session.get(f"{db_instance}{url}", headers=header) as response:
            if response.status != 200:
                text = await response.text()
                print(f"Key: {key}, response: {response.status}, URL:{url}")
                return key, f"Error {response.status}:{text}"
            data = await response.json()

            print(f"Key: {key}, response: {response.status}, URL:{url}")

            #Same Order as api_endpoints - TODO - maybe alphabetical will make more sense to other readers and will be easier to sort information...
            #Compute
            if key == 'clusters':
                return key, len(data.get('clusters', []))
            elif key == 'cluster_policies':
                return key, len(data.get('policies', []))
            elif key == 'instance_pools':
                return key, len(data.get('instance_pools', []))
            #Notebooks
            elif key == 'notebooks':
                count = sum(1 for obj in data.get('objects', []) if obj['object_type'] == 'NOTEBOOK')
                return key, count
            elif key == 'databricks_repos':
                return key, len(data.get('repos', []))
            #Jobs
            elif key == 'jobs':
                return key, len(data.get('jobs', []))
            elif key == 'dlt_pipelines':
                return key, len(data.get('statuses', []))
            #MLFlow
            elif key == 'mlflow_models':
                return key, len(data.get('registered_models', []))
            elif key == 'mlflow_experiments':
                return key, len(data.get('experiments', []))
            #Workspace
            elif key == 'workspace_conf': 
                return key, len(data)
            elif key == 'global_init_scripts': 
                return key, len(data.get('scripts', []))
            elif key == 'workspace_root':
                count = sum(1 for obj in data.get('objects', []) if obj['object_type'] != 'DIRECTORY')
                return key, count
            #DBSQL
            elif key == 'dbsql_endpoint':
                return key, len(data.get('warehouses', []))
            elif key == 'dbsql_global_config':
                return key, len(data.get('scripts', []))
            elif key == 'dbsql_dashboard':
                return key, len(data.get('dashboards', [])) 
            elif key == 'dbsql_alert':
                return key, len(data.get('results', []))
    except Exception as e:
        return key, f"Error: {e}"

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_count(session, key, url) for key, url in api_endpoints.items()]
        results = await asyncio.gather(*tasks)

        print("DB Workspace Object Counts")
        for key, value in results:
            if isinstance(value, int):
                print(f"{key}: {value}")

if __name__ == "__main__":
    if not db_token:
        print("set db token")
    else:
        asyncio.run(main())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Following to Get More Granular Details
# MAGIC The majority of the code following the initial block requires some loops to find more granular information.  Typically when there are more than 1 type.  So managed vs. external tables, single v. multi task jobs, etc.

# COMMAND ----------

#Run this for Split of Managed Tables and External Tables.  This is the bottleneck in the aforementioned code so I split it out.  
import requests
import re

#Change per catalog/schema they want to evaluate here.  Alternatively, just create this into one massive function, get the list of catalog/schema combos and then iterate it over and over again and output it to a delta table.  
#TODO - if I have time, maybe I can just do this? - there might be a more efficient way to do this other than just brute forcing it.
catalog = 'hive_metastore'
schema =  'default'

#Don't know why there was a tab when it generated the URL leveraging the original f string so I separated it out into a regular URL and regex any spaces.  
url = f"{db_instance}{api_endpoints['uc_metastore_tables']}?catalog_name={catalog}&schema_name={schema}"
clean_url = re.sub(r'\s+', '', url)

response = requests.get(clean_url, headers=header)
data = response.json()

tables = data.get('tables', [])
managed_table_count = sum(1 for table in tables if table.get('table_type') == 'MANAGED')
external_table_count = sum(1 for table in tables if table.get('table_type') == 'EXTERNAL')

display({"MANAGED": managed_table_count, "EXTERNAL": external_table_count})

# COMMAND ----------

#Run this for Split of Multi-Task and Single Task Jobs.  This is the bottleneck in the aforementioned code so I split it out.  

import requests

response = requests.get(f"{db_instance}{api_endpoints['jobs']}", headers=header)
data = response.json()
jobs = data.get('jobs', [])
multi_task_count = sum(1 for job in jobs if job.get('settings', {}).get('format') == 'MULTI_TASK')
single_task_count = sum(1 for job in jobs if job.get('settings', {}).get('format') == 'SINGLE_TASK')
display({"MULTI_TASK": multi_task_count, "SINGLE_TASK": single_task_count})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Miscellaneous

# COMMAND ----------

# List all mount points
# Courtesy of Dipankar Kushari - not everything has to be an API call and we can leverage UCX code base to get any non-UC objects. https://github.com/databrickslabs/ucx
mounts = dbutils.fs.ls("/mnt/")
display(mounts)