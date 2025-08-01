# Databricks notebook source
# DBTITLE 1,Library Imports
import json, requests, os
from typing import Dict, Any

# COMMAND ----------

# DBTITLE 1,Set Local Parameters
databricks_instance = str(spark.conf.get("spark.databricks.workspaceUrl"))
print(f"databricks_instance: {databricks_instance}")
databricks_token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
folder_path = "workflows_downloaded"

# COMMAND ----------

def load_json_folder_to_dict(folder_path: str, strip_extension: bool = False) -> Dict[str, Any]:
    """
    Load all JSON files from a folder into a dictionary.
    Args:
        folder_path (str): Path to the folder containing JSON files.
        strip_extension (bool): If True, removes the '.json' extension 
                                from dictionary keys. Default is False.
    Returns:
        Dict[str, Any]: Dictionary where keys are filenames (with or without
                        '.json' extension) and values are the loaded JSON contents.
    Raises:
        FileNotFoundError: If the folder path does not exist.
        json.JSONDecodeError: If a file cannot be parsed as JSON.
    """
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    json_dict = {}

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r") as f:
                data = json.load(f)
                key = os.path.splitext(filename)[0] if strip_extension else filename
                json_dict[key] = data
    return json_dict

workflows_data = load_json_folder_to_dict(folder_path, strip_extension=True)
print(workflows_data)

# COMMAND ----------

def deploy_workflow(dbricks_instance=None, dbricks_pat=None, workflow_def=None, overwrite=False):
    """
    Deploy a Databricks workflow (Job).
    If overwrite=True and the workflow already exists, it will be reset.
    If overwrite=False and the workflow already exists, no changes will be made.
    Parameters:
        dbricks_instance (str): Databricks workspace hostname (e.g., "adb-1234567890.11.azuredatabricks.net")
        dbricks_pat (str): Databricks Personal Access Token
        workflow_def (dict): Workflow/job definition payload (per Databricks Jobs API schema)
                             Expected structure matches Databricks Jobs API (jobs/create).
        overwrite (bool): Whether to overwrite an existing workflow (default: False)
    Returns:
        dict: API response JSON with job_id or error details
    """
    api_base = f"https://{dbricks_instance}/api/2.1/jobs"
    headers = {"Authorization": f"Bearer {dbricks_pat}", "Content-Type": "application/json"}

    job_name = workflow_def.get("name")
    if not job_name:
        raise ValueError("workflow_def must include a 'name' field")

    try:
        # --- Step 1: Look for existing job ---
        list_resp = requests.get(f"{api_base}/list", headers=headers)
        list_resp.raise_for_status()
        jobs = list_resp.json().get("jobs", [])
        existing = next((j for j in jobs if j["settings"]["name"] == job_name), None)

        if existing:
            if overwrite:
                # --- Step 2a: Reset existing job ---
                payload = {"job_id": existing["job_id"], "new_settings": workflow_def}
                response = requests.post(f"{api_base}/reset", headers=headers, data=json.dumps(payload))
                response.raise_for_status()
                print(f"✅ Overwrote existing workflow: {job_name} (job_id={existing['job_id']})")
                return response.json()
            else:
                # --- Step 2b: Skip if overwrite not requested ---
                print(f"⚠️ Workflow already exists and overwrite=False: {job_name} (job_id={existing['job_id']})")
                return {"job_id": existing["job_id"], "status": "exists"}
        else:
            # --- Step 3: Create new job ---
            response = requests.post(f"{api_base}/create", headers=headers, data=json.dumps(workflow_def))
            response.raise_for_status()
            print(f"✅ Created new workflow: {job_name}")
            return response.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR deploying workflow '{job_name}': {str(e)}")
        if 'response' in locals() and response is not None:
            print(f"Response: {response.text}")
        return {}


# Example usage: overwrite existing jobs
for workflow_name, workflow_def in workflows_data.items():
    workflow_def["settings"]["name"] = f"test_deploy_{workflow_name}"
    result = deploy_workflow(databricks_instance, databricks_token, workflow_def["settings"], overwrite=True)
    print(f"workflow_name: {workflow_name}, result: {result}")

# COMMAND ----------

