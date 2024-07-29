# Databricks notebook source
# DBTITLE 1,Library Imports
import os, requests, json
from base64 import b64encode
from pyspark.sql.functions import explode, col, lit

# COMMAND ----------

# DBTITLE 1,User Defined Parameters
# connection parameters
DATABRICKS_HOST = "https://accounts.cloud.databricks.com"
ACCOUNT_ID = "0d26daa6-5e44-4c97-a497-ef015f91254a"
CLIENT_ID = ""
CLIENT_SECRET = ""

# unity catalog parameters
catalog_name = "altmiller"
schema_name = "altmiller-schema"
table_name = "users_and_group"
full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"

# COMMAND ----------

# DBTITLE 1,Get Databricks Authentication Token
def get_databricks_token(account_id, client_id, client_secret):
    """get databricks token for a service principal"""
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic " + b64encode(f"{client_id}:{client_secret}".encode()).decode()
    }
    payload = {
        "grant_type": "client_credentials",
        "scope": "all-apis"
    }
    url_endpoint = f"https://accounts.cloud.databricks.com/oidc/accounts/{account_id}/v1/token"
    response = requests.post(url_endpoint, headers = headers, data = payload)
    response_data = json.loads(response.text) 
    if "access_token" in response_data:
        return response_data["access_token"]
    else:
        raise Exception(f"Failed to create access token: {response.text}")

# get and set databricks account level token for service principal
DATABRICKS_TOKEN = get_databricks_token(ACCOUNT_ID, CLIENT_ID, CLIENT_SECRET)
print(f"DATABRICKS_TOKEN: {DATABRICKS_TOKEN}")

# COMMAND ----------

# DBTITLE 1,Get Group and User Information at the Account Level Using SCIM
def get_all_groups(account_id, token, filter_value = None):
    """get all SCIM level groups from Account console"""

    # headers for authentication
    headers = {
        "Authorization": f"Bearer {token}"
    }
    if filter_value != None:
        url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups/?filter={filter_value}"
    else: url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups"
    response = requests.get(url_endpoint, headers = headers)
    if response.status_code == 200:
        return json.dumps(response.json()["Resources"])
    else:
        raise Exception(f"Failed to retrieve groups: {response.text}")


def get_all_users(account_id, token, filter_value = None):
    """get all SCIM level groups from Account console"""

    # headers for authentication
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


def get_user_groups(account_id, token, email_address):
    """get all groups a user is in"""

    # get user id details using Databricks SCIM rest api
    filter_value = f'userName eq {email_address}' # or None
    user_id = json.loads(get_all_users(account_id, token, filter_value))[0]["id"]
    
    # get group details using Databricks SCIM rest api
    filter_value = None #'displayName eq admins'
    groups = json.loads(get_all_groups(account_id, token, filter_value))

    # put groups and users into a Python list
    member_groups = []
    for group in groups:
        if "members" in group and user_id in str(group):
            member_groups.append(group["displayName"])
    return [{'userName': email_address, 'userId': user_id, 'groups': member_groups}]


user_groups = get_user_groups(ACCOUNT_ID, DATABRICKS_TOKEN, "robert.altmiller@databricks.com")
print(user_groups)

# COMMAND ----------

# DBTITLE 1,Add / Remove a User from an Account Level Group Using SCIM
def add_remove_user_from_group(account_id, token, group_name, email_address, action):
    """Add user to a SCIM level group from Account console"""

    # Headers for authentication
    headers = {
        "Content-type": "application/scim+json",
        "Authorization": f"Bearer {token}"
    }

    # Get user id details using Databricks SCIM rest api
    filter_value = f'userName eq "{email_address}"'
    user_id = json.loads(get_all_users(account_id, token, filter_value))[0]["id"]
    print(f"user_id: {user_id}")

    # Get group details using Databricks SCIM rest api
    filter_value = f'displayName eq "{group_name}"'
    group_id = json.loads(get_all_groups(account_id, token, filter_value))[0]["id"]
    print(f"group_id: {group_id}")

    payload = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
            {
                "op": action,
                "path": "members",
                "value": [
                    {
                        "value": user_id,
                        "display": email_address,
                        "$ref": f"Users/{user_id}"
                    }
                ]
            }
        ]
    }
    
    url_endpoint = f"{DATABRICKS_HOST}/api/2.0/accounts/{account_id}/scim/v2/Groups/{group_id}"
    response = requests.patch(url_endpoint, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return f"{action} {email_address} to group {group_name} successfully: {response.json()}"
    else:
        raise Exception(f"Failed to {action} {email_address} to group {group_name}: {response.text}")


# add a user to an account level group
result = add_remove_user_from_group(ACCOUNT_ID, DATABRICKS_TOKEN, "abc", "robert.altmiller@databricks.com", "Add")
print(result)


# remove a user from an account level group
result = add_remove_user_from_group(ACCOUNT_ID, DATABRICKS_TOKEN, "abc", "robert.altmiller@databricks.com", "Remove")
print(result)

# COMMAND ----------

# DBTITLE 1,Put Users and Groups in a Spark Dataframe
# get group details using Databricks SCIM rest api
filter_value = None #'displayName eq admins'
groups = json.loads(get_all_groups(ACCOUNT_ID, DATABRICKS_TOKEN, filter_value))

# put groups and users into a Python list
member_groups = []
for group in groups:
    if "members" in group:
        member_groups.append(group)

df_user_groups = spark.createDataFrame(member_groups)
df_user_groups = df_user_groups \
    .withColumn("members", explode(df_user_groups.members)) \
    .withColumnRenamed("displayName", "group_name") \
    .withColumnRenamed("id", "group_name_id") \
    .withColumn("display_name", col("members")["display"]) \
    .withColumn("display_name_id", col("members")["value"]) \
    .drop("members", "roles")

# COMMAND ----------

# DBTITLE 1,Write Users and Groups Spark Dataframe to a Delta Table
spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
df_user_groups.write.format("delta").saveAsTable(full_table_name)

# COMMAND ----------

# DBTITLE 1,QUERY DELTA TABLE - Get All Users and Groups
results = spark.sql(
  f"""
    SELECT * FROM {full_table_name}
  """
)
display(results)

# COMMAND ----------

# DBTITLE 1,QUERY DELTA TABLE - Get All Groups and Count of Users in Each Group
results = spark.sql(
  f"""
    SELECT group_name, COUNT(display_name) AS total_users
    FROM {full_table_name}
    GROUP BY group_name
  """
)
display(results)

# COMMAND ----------

# DBTITLE 1,QUERY DELTA TABLE - Get All Groups for a Single User
# enter a user name
user_name = 'Robert A'

results = spark.sql(
  f"""
    SELECT display_name, display_name_id, group_name
    FROM {full_table_name}
    WHERE display_name = '{user_name}'
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
