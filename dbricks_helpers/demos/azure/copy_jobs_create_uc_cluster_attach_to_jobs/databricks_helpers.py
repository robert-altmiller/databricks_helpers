# Databricks notebook source
# DBTITLE 1,Library Imports
import requests, json, time
from azure.identity import ClientSecretCredential

# COMMAND ----------

# DBTITLE 1,Get a Databricks Workspace Token
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
    if "access_token" in response_data:
        return response_data["access_token"]
    else: 
        raise Exception("Could not get access token")

# COMMAND ----------

# DBTITLE 1,Check if a Databricks Cluster Already Exists
def check_cluster_exists(workspace_url, token, cluster_name):
    """check if a Databricks cluster with the given name exists"""
    
    # Endpoint URL
    url = f"{workspace_url}/api/2.0/clusters/list"
    
    # Set headers for the request (using token for authentication)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    # Make the request
    response = requests.get(url, headers=headers)
    
    # Ensure the request was successful
    response.raise_for_status()

    # Parse the JSON response
    data = response.json()
    
    # Iterate through the clusters and check if the target cluster name exists
    for cluster in data.get('clusters', []):
        if cluster.get('cluster_name') == cluster_name:
            return True
    return False

# COMMAND ----------

# DBTITLE 1,Get Databricks Cluster ID From a Cluster Name
def get_databricks_cluster_id(token, workspace_url, cluster_name):
    """retrieve the Databricks cluster ID based on a cluster name"""
    
    # Endpoint URL
    url = f"{workspace_url}/api/2.0/clusters/list"
    
    # Set headers for the request (using token for authentication)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    # Make the request
    response = requests.get(url, headers=headers)
    
    # Ensure the request was successful
    response.raise_for_status()
    
    # Parse the JSON response
    data = response.json()
    
    # Iterate through the clusters and match the cluster name to retrieve its ID
    for cluster in data.get('clusters', []):
        if cluster.get('cluster_name') == cluster_name:
            return cluster.get('cluster_id')
    return None

# COMMAND ----------

# DBTITLE 1,Create Databricks Cluster
def create_databricks_cluster(token, workspace_url, cluster_name, payload):
    """create a databricks cluster in single user isolation mode and install specified Python libraries."""

    # Endpoint URL
    url = f"{workspace_url}/api/2.0/clusters/create"
    
    # Headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Make the API request
    response = requests.post(url, headers = headers, data = payload)
    return response.text

# COMMAND ----------

# DBTITLE 1,Get Cluster Creation Status and Wait For it to Start
def get_cluster_status(cluster_id, token, workspace_url):
    """get status of cluster creation"""
    
    # Endpoint URL
    url = f"{workspace_url}/api/2.0/clusters/get"
    
    # Headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    while True: # while cluster is still intializing
        cluster_info = requests.get(url, headers = headers, json = {'cluster_id': cluster_id}).json()
        state = cluster_info['state']
        if state == 'RUNNING':
            print(f"cluster {cluster_id} is now running...")
            break
        elif state in ['TERMINATED', 'ERROR', 'UNKNOWN']:
            print(f"cluster entered a non-recoverable state: {state}...")
            break
        print(f"current cluster {cluster_id} state: {state}. Waiting...")
        time.sleep(10)  # poll every 30 seconds

# COMMAND ----------

# DBTITLE 1,Install Python Libraries on the Cluster
def install_libraries_on_databricks(cluster_id, token, workspace_url, libraries):
    """install python libraries from PyPI onto a databricks cluster."""

    # Endpoint url
    url = f"{workspace_url}/api/2.0/libraries/install"

    # Prepare headers for the request
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # create library payload
    libraries_payload = [{"pypi": {"package": lib}} for lib in libraries]

    payload = {
        "cluster_id": cluster_id,
        "libraries": libraries_payload
    }

    # Send the request to install the libraries
    response = requests.post(url, headers = headers, json = payload)

    # Return the response
    return response

# COMMAND ----------

# DBTITLE 1,Get Databricks Job Details (Job ID From Job Name / All Job Names & Job IDs)
def get_job_details(token, workspace_url, job_name = None, return_all_jobs = False):
    """
    retrieve a databricks jobs details based on the job's name
    returns job_id with a provided job_name
    return all job_names and job_ids if return_all_jobs is True
    """
    
    # Endpoint URL
    url_base = f"{workspace_url}/api/2.1/jobs/list"
    url = url_base

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    job_counter = 0
    job_id = None
    has_more = True
    all_jobs = {}

    while has_more: # iterate through all the databricks jobs
    
        response = requests.get(url, headers = headers)
        response_json = response.json()
        has_more = response_json.get("has_more")
        
        if has_more == True: # more jobs exist
            next_page_token = response_json.get("next_page_token")
            url = f"{url_base}?page_token={next_page_token}"

        else: has_more = False # no more jobs

        if response.status_code == 200:
            jobs = response_json.get("jobs", [])
            for job in jobs:
                job_name_response = job.get("settings", {}).get("name")
                if job_name_response == job_name:
                    job_id = job["job_id"]
                job_counter += 1
                all_jobs[job_name_response] = job["job_id"]
        else:
            response.raise_for_status()

    print(f"total_scanned_jobs: {job_counter}")
    if job_name != None:
        return job_id
    else: return all_jobs

# COMMAND ----------

# DBTITLE 1,Get Databricks Job Config
def get_databricks_job_config(token, workspace_url, job_id):
    """fetch the configuration of a specified Databricks job"""
    
    # Endpoint URL
    url = f"{workspace_url}/api/2.0/jobs/get?job_id={job_id}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# COMMAND ----------

# DBTITLE 1,Copy a Databricks Job
def copy_databricks_job(token, workspace_url, job_config, new_job_name):
    """copy an existing Databricks job"""

    if new_job_name:
        config['settings']['name'] = new_job_name
    
    # Remove the 'job_id' from the configuration as it's specific to the original job
    config.pop('job_id', None)
    
    # Endpoint URL
    url = f"{workspace_url}/api/2.0/jobs/create"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers = headers, json = config['settings'])
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# COMMAND ----------

# DBTITLE 1,Delete a Databricks Job
def delete_databricks_job(token, workspace_url, job_name_to_delete):
    """delete a Databricks job if it exists based on its name"""
    
    # Endpoint URL
    url = f"{workspace_url}/api/2.0/jobs/delete"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    job_id_to_delete = get_job_details(token, workspace_url, job_name = job_name_to_delete)
    data = {"job_id": job_id_to_delete}
    response = requests.post(url, headers = headers, json = data)
    if response.status_code == 200:
        return f"Job '{job_name_to_delete}' with ID {job_id_to_delete} has been deleted\n"
    else:
        response.raise_for_status()

# COMMAND ----------

# DBTITLE 1,Attach a Cluster to a Databricks Job
def attach_cluster_to_job(token, workspace_url, job_id, cluster_id, client_id):
    """attach a cluster to a databricks job"""
    
    # Endpoint URL
    url = f"{workspace_url}/api/2.0/jobs/update"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Request payload
    payload = {
        "job_id": job_id,
        "new_settings": {
            "run_as": {
                "service_principal_name": client_id    
            },
            "existing_cluster_id": cluster_id,
        }
    }
    
    response = requests.post(url, headers = headers, json = payload)
    if response.status_code == 200:
        return response
    else:
        response.raise_for_status()
