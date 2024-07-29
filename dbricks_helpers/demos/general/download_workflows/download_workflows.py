# Databricks notebook source
# DBTITLE 1,Library Imports
import json, requests

# COMMAND ----------

# DBTITLE 1,Set Local Parameters
databricks_instance = str(spark.conf.get("spark.databricks.workspaceUrl"))
databricks_token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())

# COMMAND ----------

# DBTITLE 1,Generic Functions
def write_new_file(folderpath = None, filename = None, data = None, writemethod = "w"):
    """write a new file to databricks file system"""
    try:
        filepath = f"{folderpath}/{filename}"
        f = open(filepath, writemethod) # params: a = append, w = write
        f.write(data)  
        f.close()
        return f"filpath '{filepath}' written successfully..."
    except PermissionError:
        return f"[Errno 13] Permission denied for path '{filepath}'"

# COMMAND ----------

# DBTITLE 1,List Workflow Jobs
def list_workflows(dbricks_instance = None, dbricks_pat = None):
    """list all workflows"""
    jsondata = None
    api_url = f"https://{dbricks_instance}/api/2.0/jobs/list"
    headers = {"Authorization": f"Bearer {dbricks_pat}", "Content-Type": "application/json"}
    payload = None
    try:
        response = requests.get(api_url, headers = headers, data = payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: {str(e)}")
    return response.json()["jobs"]

workflows = json.dumps(list_workflows(databricks_instance, databricks_token))

# get workflow job ids
job_ids = []
for workflow in json.loads(workflows):
    job_ids.append(workflow["job_id"])

# COMMAND ----------

# DBTITLE 1,Get Workflow Details and Download in Json Files
def get_workflow_job_details(dbricks_instance = None, dbricks_pat = None, job_id = None):
    """get job workflow job details"""
    api_url = f"https://{dbricks_instance}/api/2.0/jobs/get?job_id={str(job_id)}"
    headers = {"Authorization": f"Bearer {dbricks_pat}", "Content-Type": "application/json"}
    payload = None
    try:
        response = requests.get(api_url, headers = headers, data = payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: {str(e)}")
    return response.json()


# download all workflows locally
dbfs_path = "./workflows_downloaded"
for jobid in job_ids:
    wf_details = json.dumps(get_workflow_job_details(databricks_instance, databricks_token, jobid))
    wf_details_json = json.loads(wf_details)
    wf_name = wf_details_json["settings"]["name"]
    result = write_new_file(dbfs_path, f"{wf_name}.json", wf_details, "w")
    print(result)
