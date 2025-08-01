# Databricks notebook source
# DBTITLE 1,Library Imports
import json, requests, os, shutil

# COMMAND ----------

# DBTITLE 1,Set Local Parameters
databricks_instance = str(spark.conf.get("spark.databricks.workspaceUrl"))
databricks_token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())

# COMMAND ----------

# DBTITLE 1,Generic Functions
def delete_directory(folderpath: str) -> str:
    """
    Delete a directory and all its contents if it exists.
    Parameters:
        folderpath (str): Path to the directory to delete.
    Returns:
        str: Status message about the deletion.
    """
    try:
        if os.path.exists(folderpath):
            shutil.rmtree(folderpath)
            return f"✅ Directory '{folderpath}' deleted successfully."
        else:
            return f"ℹ️ Directory '{folderpath}' does not exist."
    except PermissionError:
        return f"[Errno 13] Permission denied for path '{folderpath}'"
    except Exception as e:
        return f"❌ Error deleting directory '{folderpath}': {e}"


def write_new_file(folderpath=None, filename=None, data=None, writemethod="w"):
    """
    Write a new file to the local/DBFS filesystem.
    Parameters:
        folderpath (str): Path to folder where file should be written.
        filename (str): Name of the file to create.
        data (str): Data to write into the file.
        writemethod (str): File write method ('w' = write, 'a' = append).
    Returns:
        str: Status message about file creation.
    """
    try:
        os.makedirs(folderpath, exist_ok=True)
        filepath = os.path.join(folderpath, filename)
        with open(filepath, writemethod) as f:
            f.write(data)
        return f"✅ File '{filepath}' written successfully."
    except PermissionError:
        return f"[Errno 13] Permission denied for path '{filepath}'"
    except Exception as e:
        return f"❌ Error writing file '{filepath}': {e}"


# COMMAND ----------

# DBTITLE 1,List Workflow Jobs
def list_workflows(dbricks_instance=None, dbricks_pat=None):
    """
    Retrieve a list of all Databricks workflows (jobs) in a given workspace.
    This function calls the Databricks Jobs API (`/api/2.0/jobs/list`) using the provided
    workspace instance URL and personal access token (PAT). It returns the list of jobs
    in JSON format.
    Parameters:
        dbricks_instance (str): Databricks workspace hostname 
        dbricks_pat (str): Databricks Personal Access Token used for authentication.
    Returns:
        list[dict]: A list of workflow/job definitions, each represented as a dictionary.
                    Returns an empty list if the request fails.
    Raises:
        requests.exceptions.RequestException: If the API request fails (network, auth, etc.).
    """
    jsondata = None
    api_url = f"https://{dbricks_instance}/api/2.0/jobs/list"
    headers = {"Authorization": f"Bearer {dbricks_pat}", "Content-Type": "application/json"}
    payload = None
    try:
        response = requests.get(api_url, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: {str(e)}")
        return []
    return response.json().get("jobs", [])

job_ids = list_workflows(databricks_instance, databricks_token)

# COMMAND ----------

# DBTITLE 1,Get Workflow Details and Download in Json Files
def get_workflow_job_details(dbricks_instance=None, dbricks_pat=None, job_id=None):
    """
    Retrieve detailed information for a specific Databricks workflow (job).
    This function calls the Databricks Jobs API (`/api/2.0/jobs/get`) to fetch
    the full job definition and configuration details for the given job_id.
    Parameters:
        dbricks_instance (str): Databricks workspace hostname 
        dbricks_pat (str): Databricks Personal Access Token used for authentication.
        job_id (int or str): Unique identifier of the Databricks workflow/job.
    Returns:
        dict: Workflow/job details as a dictionary (per Databricks Jobs API schema).
              Returns an empty dictionary if the request fails.
    Raises:
        requests.exceptions.RequestException: If the API request fails (network, auth, etc.).
    """
    api_url = f"https://{dbricks_instance}/api/2.0/jobs/get?job_id={str(job_id)}"
    headers = {"Authorization": f"Bearer {dbricks_pat}", "Content-Type": "application/json"}
    payload = None
    try:
        response = requests.get(api_url, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: {str(e)}")
        return {}
    return response.json()


# download all workflows locally
dbfs_path = "./workflows_downloaded"
delete_directory(dbfs_path)
for jobid in job_ids:
    jobid = jobid["job_id"]
    wf_details = json.dumps(get_workflow_job_details(databricks_instance, databricks_token, jobid))
    wf_details_json = json.loads(wf_details)
    wf_name = wf_details_json["settings"]["name"]
    result = write_new_file(dbfs_path, f"{wf_name}.json", wf_details, "w")
    print(result)

# COMMAND ----------

