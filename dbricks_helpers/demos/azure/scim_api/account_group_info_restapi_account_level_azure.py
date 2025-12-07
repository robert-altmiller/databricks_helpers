# Databricks notebook source
# DBTITLE 1,Library Imports
import os, requests, json
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import explode, col, lit, collect_list, map_from_arrays

# COMMAND ----------

# DBTITLE 1,User Defined Parameters
# connection parameters
DATABRICKS_HOST = "https://accounts.azuredatabricks.net"
TENANT_ID = "MY_AZURE_TENANT_ID"
ACCOUNT_ID = "MY_AZURE_DATABRICKS_ACCOUNT_ID"
CLIENT_ID = "MY_AZURE_CLIENT_ID"
CLIENT_SECRET = "MY_AZURE_CLIENT_SECRET"

# unity catalog parameters
catalog_name = "MY_CATALOG"
schema_name = "MY_SCHEMA"
table_name = "all_users_and_groups"
full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"

# COMMAND ----------

# DBTITLE 1,Get Databricks Authentication Token
def get_databricks_token(tenant_id, client_id, client_secret):
    """
    Obtain an Azure Databricks access token for a service principal using Microsoft Entra ID (Azure AD).
    Args:
        tenant_id (str): The Azure AD tenant (directory) ID.
        client_id (str): The application (client) ID of the service principal.
        client_secret (str): The client secret for the service principal.
    Returns:
        str: The access token string to be used for Databricks API authentication.
    Raises:
        Exception: If the token request fails or the response does not contain an access token.
    """
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
    }
    url_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    response = requests.post(
        url_endpoint,
        headers=headers,
        data=payload
    )
    response_data = response.json()
    if "access_token" in response_data:
        return response_data["access_token"]
    else:
        raise Exception(
            f"Failed to create access token: {response.text}"
        )

DATABRICKS_TOKEN = get_databricks_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
print(f"DATABRICKS_TOKEN: {DATABRICKS_TOKEN}")

# COMMAND ----------

# DBTITLE 1,Get Group Users at the Account Level Using SCIM
def get_all_users(account_id, token, filter_value = None):
    """
    Get all SCIM level groups from Account console.
    """
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}"
    }
    if filter_value != None:
        url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Users/?filter={filter_value}"
    else: url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Users"
    response = requests.get(url_endpoint, headers = headers)
    if response.status_code == 200:
        return json.dumps(response.json()["Resources"])
    else:
        raise Exception(f"Failed to retrieve users: {response.text}")

# Get user id details using Databricks SCIM rest api
users_dict = {}
filter_value = None #f'userName eq "raltmiller@digitalaviationservices.com"'
users = json.loads(get_all_users(ACCOUNT_ID, DATABRICKS_TOKEN, filter_value))
for user in users: 
    users_dict[user['id']] = [user['userName'], "user"]
print(users_dict)

# COMMAND ----------

# DBTITLE 1,Get All Groups at the Account Level Using SCIM
def get_all_groups(account_id, token, filter_value = None):
    """
    Get all SCIM level groups from Account console.
    """
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}"
    }
    if filter_value != None:
        url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups/?filter={filter_value}"
    else: url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups"
    try:
        response = requests.get(url_endpoint, headers = headers)
        if response.status_code == 200:
            return json.dumps(response.json()["Resources"])
    except:
        raise Exception(f"Failed to retrieve groups: {response.text}")

# Get group id details using Databricks SCIM rest api
groups_dict = {}
filter_value = None #'displayName eq "datamesh-bdmp-admins-dev"'
groups = json.loads(get_all_groups(ACCOUNT_ID, DATABRICKS_TOKEN, filter_value))
for group in groups: 
    groups_dict[group['id']] = [user['displayName'], "group"]
print(groups_dict)

# COMMAND ----------

# DBTITLE 1,Get All SPs at the Account Level Using SCIM
def get_all_service_principals(account_id, token, filter_value=None):
    """
    Get all SCIM service principals from Account console.
    """
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {token}"
    }
    if filter_value:
        url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals?filter={filter_value}"
    else:
        url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals"
    response = requests.get(url_endpoint, headers=headers)
    if response.status_code == 200:
        return json.dumps(response.json()["Resources"])
    else:
        raise Exception(f"Failed to retrieve service principals: {response.text}")

# Get service principal id details using Databricks SCIM rest api
sp_dict = {}
filter_value = None #'displayName eq "temp-testing"'
service_principals = json.loads(get_all_service_principals(ACCOUNT_ID, DATABRICKS_TOKEN, filter_value))
for sp in service_principals:
    sp_dict[sp['id']] = [user['displayName'], "service_principal"]
print(sp_dict)

# COMMAND ----------

# DBTITLE 1,Get Group Membership at the Account Level Using SCIM
def fetch_group_members(group, account_id, token):
    group_id = group["id"]
    group_name = group["displayName"]
    print(f"processing group name '{group_name}' and group id '{group_id}'....")
    url = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    rows = []
    if response.status_code == 200:
        group_detail = response.json()
        members = group_detail.get("members", [])
        for member in members:
            rows.append({
                "group_name": group_detail.get("displayName"),
                "group_id": group_id,
                "member_id": member.get("value"),
                "member_display": member.get("display")
            })
    else:
        rows.append({
            "group_name": group_name,
            "group_id": group_id,
            "member_id": None,
            "member_display": None
        })
    return rows

def get_group_members(account_id, token, max_workers=10):
    group_member_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_group_members, group, account_id, token) for group in groups]
        for future in as_completed(futures):
            group_member_rows.extend(future.result())
    return group_member_rows

group_members = get_group_members(ACCOUNT_ID, DATABRICKS_TOKEN)

# COMMAND ----------

# DBTITLE 1,Put Users, Groups, and Entity Types in a Spark Dataframe
# Add member email and entity_type using member_id
for group_member in group_members:
    member_id = group_member.get("member_id")
    if member_id in users_dict:
        group_member["member_email"] = users_dict[member_id][0]
        group_member["entity_type"] = users_dict[member_id][1]
    elif member_id in groups_dict:
        group_member["entity_type"] = groups_dict[member_id][1]
    elif member_id in sp_dict:
        group_member["entity_type"] = sp_dict[member_id][1]
    else:
        group_member["entity_type"] = None

# Put users and groups into a spark dataframe
df_user_groups = spark.createDataFrame(group_members)
df_user_groups = (
    df_user_groups
    .groupBy("member_id", "member_display", "member_email", "entity_type")
    .agg(
        collect_list("group_name").alias("group_names"),
        collect_list("group_id").alias("group_ids")
    )
)
display(df_user_groups)

# COMMAND ----------

# DBTITLE 1,Add / Remove a User from an Account Level Group Using SCIM
def add_remove_user_from_group(account_id, token, group_name, email_address, action):
    """
    Add or remove a user to/from a SCIM group at account level in Azure Databricks.
    """
    # Headers for authentication
    headers = {
        "Content-type": "application/scim+json",
        "Authorization": f"Bearer {token}"
    }
    # Get user ID
    filter_value = f'userName eq "{email_address}"'
    users = json.loads(get_all_users(account_id, token, filter_value))
    if not users:
        raise Exception(f"User {email_address} not found")
    user_id = users[0]["id"]

    # Get group ID
    filter_value = f'displayName eq "{group_name}"'
    groups = json.loads(get_all_groups(account_id, token, filter_value))
    if not groups:
        raise Exception(f"Group {group_name} not found")
    group_id = groups[0]["id"]

    # Patch payload
    if action.lower() == "add":
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "add",
                    "path": "members",
                    "value": [
                        {"value": user_id}
                    ]
                }
            ]
        }
    elif action.lower() == "remove":
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "remove",
                    "path": f"members[value eq \"{user_id}\"]"
                }
            ]
        }
    else:
        raise ValueError("Action must be either 'add' or 'remove'")
    
    url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
    response = requests.patch(url_endpoint, headers=headers, data=json.dumps(payload))
    if int(response.status_code) > 200:
        return f"{action} {email_address} to/from group {group_name} successfully: {response.status_code}"
    else:
        raise Exception(f"Failed to {action} {email_address} to group {group_name}: {response.text}")

# Add a user from an account level group    
# user_email = "raltmiller@digitalaviationservices.com"
# result = add_remove_user_from_group(ACCOUNT_ID, DATABRICKS_TOKEN, "datamesh-bdmp-admins-dev", user_email, "add")
# print(result)

# Remove a user from an account level group
# result = add_remove_user_from_group(ACCOUNT_ID, DATABRICKS_TOKEN, "datamesh-bdmp-admins-dev", user_email, "remove")
# print(result)

# COMMAND ----------

# DBTITLE 1,Write Users and Groups Spark Dataframe to a Delta Table
spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
df_user_groups.write.mode("overwrite").format("delta").saveAsTable(full_table_name)

# COMMAND ----------

# DBTITLE 1,QUERY DELTA TABLE - Get All Users and Groups
results = spark.sql(
  f"""
    SELECT * FROM {full_table_name}
  """
)
display(results)

# COMMAND ----------

# DBTITLE 1,QUERY DELTA TABLE - Get All Groups for a Single User
# enter a user name
user_name = 'Robert Altmiller'

results = spark.sql(
  f"""
    SELECT *
    FROM {full_table_name}
    WHERE member_display = '{user_name}'
  """
)
display(results)

# COMMAND ----------

# MAGIC %md # Document references
# MAGIC
# MAGIC - https://docs.databricks.com/en/dev-tools/python-sql-connector.html
# MAGIC - https://docs.databricks.com/api/account/accountgroups/delete
# MAGIC - https://databricks-sdk-py.readthedocs.io/en/latest/
# MAGIC - https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-groups.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Changes and Functionality Below

# COMMAND ----------

# DBTITLE 1,Get a All Groups and and Group IDs Only
groups = spark.sql(f"""
  SELECT member_id AS group_id, member_display as group_name 
  FROM {full_table_name} 
  WHERE entity_type = 'group' 
  ORDER BY member_display ASC
""").collect()

# Convert to list of dictionaries: [{group_id: group_name}, ...]
groupid_and_group = [{group['group_id']: group['group_name']} for group in groups]
print(groupid_and_group)

# COMMAND ----------

# DBTITLE 1,Create New Group With Members
def create_new_group(account_id, token, group_name, group_id, members=None):
    """
    Create a duplicate Databricks SCIM group (appending _new),
    add selected members, and optionally remove those members from the original group.
    """
    new_groupname = f"{group_name}_new"
    # Headers for authentication
    headers = {
        "Content-type": "application/scim+json",
        "Authorization": f"Bearer {token}"
    }

    # Build members list for the new group (must be list of dicts with 'value' key)
    members_body = []
    for member in members:
        if member["group_id"] == group_id:
            members_body.append({"value": member["member_id"]})
    # SCIM payload for new group creation (correct key is 'members')
    payload = {
        "displayName": new_groupname,
        "members": members_body
    }
    
    print(f"Creating new group: {new_groupname}")
    url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups"
    response = requests.post(url_endpoint, json=payload, headers=headers)
    if response.status_code <= 204:
       new_group_name = response.json().get("displayName")
       new_group_id = response.json().get("id")
       print(f"New Group '{new_group_name}' created successfully with ID '{new_group_id}'...\n")
    else :
        print(f"Error creating new group: {response.status_code} {response.text}")
        return None
    return new_group_id

# Unit Test
for group in groupid_and_group:
    for group_id, group_name in group.items():
        new_group_id = create_new_group(ACCOUNT_ID, DATABRICKS_TOKEN, group_name, group_id, group_members, False)
    break # need break here or it will iterate through all the groups!!

# COMMAND ----------

# DBTITLE 1,List Group Members
# Function to list all members in a group by group name or group id
def list_group_members(account_id, token, group_name=None, group_id=None):
    """
    List all members in a Databricks account-level group by group name or group id.
    Returns a list of dicts with member id and display name.
    """
    headers = {"Authorization": f"Bearer {token}"}
    if not group_id:
        # Use get_group_id_by_name from previous cell
        group_id = get_group_id_by_name(account_id, token, group_name)
        if not group_id:
            print(f"Group '{group_name}' not found.")
            return []
    url = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        group_detail = response.json()
        members = group_detail.get("members", [])
        return [{"member_id": m.get("value"), "member_display": m.get("display")} for m in members]
    else:
        print(f"Failed to fetch group members: {response.status_code} {response.text}")
        return []

list_group_members(ACCOUNT_ID, DATABRICKS_TOKEN, group_name = "YOUR_GROUP_NAME")

# COMMAND ----------

# DBTITLE 1,Delete a Group with a Group Name
def get_group_id_by_name(account_id, token, group_name):
    """
    Get the group ID for a given group name at the Databricks account level using the SCIM API.
    Args:
        account_id (str): Databricks account ID.
        token (str): Databricks PAT or Azure AD token.
        group_name (str): The group name to look up.
    Returns:
        str: The group ID if found, else None.
    """
    filter_value = f'displayName eq "{group_name}"'
    groups = json.loads(get_all_groups(account_id, token, filter_value))
    if groups:
        return groups[0]["id"]
    else:
        return None

def delete_group(account_id, token, group_id):
    """
    Delete a group at the Databricks account level using the SCIM API.
    Args:
        account_id (str): Databricks account ID.
        token (str): Databricks PAT or Azure AD token.
        group_id (str): The group ID to delete.
    Returns:
        str: Success message or raises Exception on failure.
    """
    url = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.delete(url, headers=headers)
    if response.status_code == 204:
        return f"Group {group_id} deleted successfully."
    else:
        raise Exception(f"Failed to delete group {group_id}: {response.status_code} {response.text}")

def delete_group_by_name(account_id, token, group_name):
    """
    Delete a group by its name at the Databricks account level using the SCIM API.
    Args:
        account_id (str): Databricks account ID.
        token (str): Databricks PAT or Azure AD token.
        group_name (str): The group name to delete.
    Returns:
        str: Success message or raises Exception on failure.
    """
    group_id = get_group_id_by_name(account_id, token, group_name)
    if group_id:
        return delete_group(account_id, token, group_id)
    else:
        raise Exception(f"Group '{group_name}' not found.")

# Usage example: delete by group name
group_name = 'YOUR_GROUP_NAME'
try:
    result = delete_group_by_name(ACCOUNT_ID, DATABRICKS_TOKEN, group_name)
    print(result)
except Exception as e:
    print(e)