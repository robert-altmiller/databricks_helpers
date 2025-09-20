# Databricks notebook source
# DBTITLE 1,Library Imports
import json, requests, os, copy
from typing import Dict, Any

# COMMAND ----------

# DBTITLE 1,Set Local Parameters
databricks_instance = str(spark.conf.get("spark.databricks.workspaceUrl"))
print(f"databricks_instance: {databricks_instance}")
databricks_token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
folder_path = "workflows_downloaded"
deploy_environment = "aws" # or azure
workflow_name_prefix = "test_deploy"
change_compute_to_serverless = True
overwrite_workflows = True

# COMMAND ----------

# DBTITLE 1,Read Workflow Definitions
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

# DBTITLE 1,Convert Azure Workflow for AWS
def convert_azure_to_aws_cluster(workflow_payload, default_node="i3.xlarge"):
    """
    Convert a Databricks workflow cluster definition from Azure-specific
    attributes to AWS-compatible attributes.
    Supports both:
      - Multi-task jobs with `job_clusters`
      - Single-task jobs with top-level `new_cluster`
    Steps:
      1. Removes Azure-only fields (e.g., `azure_attributes`).
      2. Ensures AWS attributes (`aws_attributes`) are present with a default spot policy.
      3. Maps Azure VM SKUs to the closest AWS instance types.
      4. Falls back to `default_node` if no mapping is found.
    Args:
        workflow_payload (dict): Workflow/job JSON definition.
        default_node (str): AWS node_type_id to use when no mapping exists.
    Returns:
        dict: Updated workflow payload ready for AWS deployment.
    Raises:
        ValueError: If neither `job_clusters` nor `new_cluster` is found.
    """
    wf = copy.deepcopy(workflow_payload)
    # Azure → AWS node type mapping
    node_map = {
        # General Purpose (Dsv5 → M5d)
        "Standard_D4ds_v5": "m5d.xlarge",
        "Standard_D8ds_v5": "m5d.2xlarge",
        "Standard_D16ds_v5": "m5d.4xlarge",
        "Standard_D32ds_v5": "m5d.8xlarge",
        "Standard_D64ds_v5": "m5d.16xlarge",

        # Memory Optimized (Esv5 → R5d)
        "Standard_E8ds_v5": "r5d.2xlarge",
        "Standard_E16ds_v5": "r5d.4xlarge",
        "Standard_E32ds_v5": "r5d.8xlarge",
        "Standard_E64ds_v5": "r5d.16xlarge",

        # Compute Optimized (Fsv2 → C5d)
        "Standard_F4s_v2": "c5d.xlarge",
        "Standard_F8s_v2": "c5d.2xlarge",
        "Standard_F16s_v2": "c5d.4xlarge",
        "Standard_F32s_v2": "c5d.9xlarge",

        # Storage Optimized (Lsv3 → I3)
        "Standard_L8s_v3": "i3.2xlarge",
        "Standard_L16s_v3": "i3.4xlarge",
        "Standard_L32s_v3": "i3.8xlarge",
        "Standard_L64s_v3": "i3.16xlarge",

        # Memory Heavy (G-series → x1 family)
        "Standard_G5": "x1.32xlarge",   # 32 vCPU, 448 GB RAM
        "Standard_GS5": "x1e.32xlarge", # 32 vCPU, 976 GB RAM (extra memory)

        # GPU (NC/ND → P3 family)
        "Standard_NC6": "p3.2xlarge",    # 1 GPU, 12 vCPU
        "Standard_NC12": "p3.8xlarge",   # 4 GPU, 48 vCPU
        "Standard_NC24": "p3.16xlarge",  # 8 GPU, 64 vCPU
        "Standard_ND40rs_v2": "p3dn.24xlarge", # ML/AI workloads

        # HPC (HB/HC → c5n or hpc families)
        "Standard_HB120rs_v2": "c5n.18xlarge",
        "Standard_HC44rs": "c5n.9xlarge",
    }

    def patch_cluster(cluster: dict):
        """
        Patch a cluster definition:
          - remove Azure-only attributes
          - add aws_attributes if missing
          - translate node_type_id if needed
        """
        cluster.pop("azure_attributes", None)
        cluster.setdefault("aws_attributes", {
            "availability": "SPOT_WITH_FALLBACK_ON_DEMAND"
        })
        node_type = cluster.get("node_type_id")
        if node_type in node_map:
            cluster["node_type_id"] = node_map[node_type]
        elif not node_type:
            cluster["node_type_id"] = default_node
        return cluster

    # Multi-task job (job_clusters block)
    if "job_clusters" in wf and wf["job_clusters"]:
        for cluster_spec in wf["job_clusters"]:
            cluster_spec["new_cluster"] = patch_cluster(cluster_spec.get("new_cluster", {}))
    # Single-task job (top-level new_cluster)
    elif "new_cluster" in wf:
        wf["new_cluster"] = patch_cluster(wf.get("new_cluster", {}))
    else:
        raise ValueError("No job_clusters or new_cluster found in workflow payload")
    return wf


def convert_job_to_serverless(settings: dict) -> dict:
    """
    Convert a Databricks job *settings* dict from using job_clusters to serverless compute.
    Args:
        settings (dict): The "settings" block of a Databricks job JSON.
    Returns:
        dict: Modified settings dict that runs all tasks on serverless.
    """
    new_settings = copy.deepcopy(settings)

    # Remove job_clusters
    new_settings.pop("job_clusters", None)

    # Replace job_cluster_key in tasks
    for task in new_settings.get("tasks", []):
        task.pop("job_cluster_key", None)
        task["compute"] = "serverless"
    return new_settings

# COMMAND ----------

# DBTITLE 1,Deploy Workflows to Azure or AWS
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


# Example usage: overwrite existing jobs or create new jobs with job name prefix
for workflow_name, workflow_def in workflows_data.items():
    workflow_def["settings"]["name"] = f"{workflow_name_prefix}_{workflow_name}"
    if deploy_environment == "azure":
        azure_workflow_payload =  workflow_def["settings"]
        if change_compute_to_serverless == True: 
            azure_workflow_payload = convert_job_to_serverless(azure_workflow_payload)
        result = deploy_workflow(databricks_instance, databricks_token, azure_workflow_payload, overwrite = overwrite_workflows)
    else: # deploy environment is aws
        if change_compute_to_serverless == True: 
            aws_workflow_payload = convert_job_to_serverless(workflow_def["settings"])
        else:
            aws_workflow_payload = convert_azure_to_aws_cluster(workflow_def["settings"])
        result = deploy_workflow(databricks_instance, databricks_token, aws_workflow_payload, overwrite = overwrite_workflows)
    print(f"workflow_name: {workflow_name}, result: {result}")