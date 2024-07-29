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

# duplicate jobs allowed flag
dups_allowed_flag = False 

# databricks job name parameters
new_job_name_prefix = "altmiller"
job_names_list = [
    "volumes_w_Tags_and_classification_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "volumes_w_starburst_MMS_SALES",
    "volumes_w_Knowledge_Graph_MMS_SALES",
    "volumes_w_glossary_json_creation_MMS_SALES",
    "volumes_w_glossary_automation_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "volumes_w_gen_lineage_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "volumes_w_dq_stats_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "volumes_w_data_profiling_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "volumes_w_cog_search_MARKET_SEGMENTATION_AND_PRODUCT_CATEGORIZATION_TO_DATE",
    "volumes_Metrics_and_cache_update"
]

# COMMAND ----------

# DBTITLE 1,Copy Databricks Jobs
# get a databricks token for a service principal
token = get_databricks_token(tenant_id, client_id, client_secret)
# get a dictionary of all job names and job ids {job_name : job_id}    
jobs_dict = get_job_details(token, workspace_url, return_all_jobs = True)
all_job_names = [k for k, v in jobs_dict.items()]

# copy all the databricks jobs in the jobs_names_list
for job_name, job_id in jobs_dict.items():
    for job_name in job_names_list: # create a copy of the job
        print(f"\n---copying the job '{job_name}'---\n")
        create_job_config = get_databricks_job_config(token, workspace_url, job_id)
        print(create_job_config)

        # check if the new_job_name already exists and delete it
        new_job_name = f"{new_job_name_prefix}_{job_name}"
        if new_job_name in all_job_names and dups_allowed_flag == False: # then delete the existing new job
            result = delete_databricks_job_if_exists(token, workspace_url, new_job_name)
            print(result)

        copy_databricks_job(token, workspace_url, create_job_config, new_job_name)
        print(f"\n---finished copying the job '{job_name}' as '{new_job_name}'---\n")
        job_names_list.remove(job_name)
        break
