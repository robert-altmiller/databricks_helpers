# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install --quiet databricks-sdk==0.21.0

# COMMAND ----------

# DBTITLE 1,Restart Python Interpreter for Databricks SDK
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
import json
from pyspark.sql.functions import explode, col, lit
from databricks.sdk import AccountClient

# COMMAND ----------

# DBTITLE 1,User Defined Parameters
# connection parameters
DATABRICKS_HOST = "https://accounts.cloud.databricks.com"
ACCOUNT_ID = "0d26daa6-5e44-4c97-a497-ef015f91254a"
CLIENT_ID = ""
CLIENT_SECRET = ""

# COMMAND ----------

# DBTITLE 1,Query for Account Groups and Users Using Databricks SDK
def get_account_client(host, account_id, client_id, client_secret):
    """get databricks sdk account client"""
    acc_client = AccountClient (
        host = host, 
        account_id = account_id, 
        client_id = client_id, 
        client_secret = client_secret
    )
    return acc_client


def get_account_groups(client):
    """get account grouos"""
    return client.groups.list()


def get_account_users(client):
    """get account users"""
    return client.users.list()


def get_complex_values_props(complex_value):
    """get ComplexValue() object properties"""
    attributes_list = []
    for attr in complex_value:
      props = vars(attr)
      for prop in props:
        attributes_list.append(prop)
      break
    return attributes_list


def get_complex_values_props_values(complex_value, prefix = ""):
    """get properties from a ComplexValue() object and values"""
    attributes_dict = {}
    for attr in complex_value:
        props = vars(attr)
        for prop in props:
            actual_value = getattr(attr, prop, "NA")
            attributes_dict[f"{prefix}{prop}"] = actual_value 
    return attributes_dict


def get_user_emails(client):
    """get user user_id and user emails"""
    users = get_account_users(client)
    users_emails = {}
    for user in users:
      user_id = user.id
      email_objs = getattr(user, "emails", None)
      for email_obj in email_objs:
        email_address = email_obj.value
      users_emails[user_id] = email_address
    return users_emails


def write_to_local(filename, contents):
  """write to a local file"""
  with open(filename, 'w') as file:
      file.write(contents)


def get_users_groups(client = None, user_email = None, find_all_users = False):
  """find all the Databricks account level groups a user is in"""
  # get groups
  groups = [grp for grp in get_account_groups(client)]
  # get group properties only and not values
  groups_props = get_complex_values_props(groups) # 
  # get groups users
  users = [usr for usr in get_account_users(client)]
  # get user emails in a dict
  users_emails = get_user_emails(client)

  # process groups into json
  groups_list = []
  members_list = []
  counter = 0
  for group in groups:
    groups_dict = {}
    for prop in groups_props: # group properties
      prop_values = getattr(group, prop, None)
      if prop == "members":
        for prop_value in prop_values:
            members_dict = {}
            members_dict[f"{prop}_display"] = str(prop_value.display)
            members_dict[f"{prop}_primary"] = str(prop_value.primary)
            members_dict[f"{prop}_ref"] = str(prop_value.ref)
            members_dict[f"{prop}_type"] = str(prop_value.type)
            members_dict[f"{prop}_val"] = str(prop_value.value)
            members_dict[f"{prop}_email"] = str(users_emails.get(prop_value.value, "NA"))

            if users_emails.get(prop_value.value, "NA") == user_email and find_all_users == False: # find single user
              members_list.append(members_dict)
              break
            elif find_all_users == True:
              members_list.append(members_dict)
        
        groups_dict[prop] = members_list
        members_list = []
      else: 
        if "ComplexValue" in str(prop_values): # expand the ComplexValue object and get properties and values
          prop_values_updated = get_complex_values_props_values(prop_values)
          groups_dict[prop] = prop_values_updated
        else: groups_dict[prop] = prop_values
    groups_list.append(groups_dict)
  return groups_list


# get user or all users groups
user_email = "robert.altmiller@databricks.com"
find_all_users = True # or False
client = get_account_client(DATABRICKS_HOST, ACCOUNT_ID, CLIENT_ID, CLIENT_SECRET)
results = get_users_groups(client = client, user_email = user_email, find_all_users = find_all_users)

# COMMAND ----------

# DBTITLE 1,Convert Results From Previous Section Into Spark Dataframe
# convert groups into spark dataframe
groups_rdd = spark.sparkContext.parallelize([json.dumps(results)])
groups_df = spark.read.json(groups_rdd)
groups_df = groups_df \
  .withColumn("members", explode(groups_df.members)) \
  .withColumn("members_display", col("members")["members_display"]) \
  .withColumn("members_email", col("members")["members_email"]) \
  .withColumn("members_id", col("members")["members_val"]) \
  .withColumnRenamed("display_name", "group_name") \
  .withColumnRenamed("id", "group_id") \
  .select("group_name", "group_id", "members_display", "members_id", "members_email", "roles")
display(groups_df.where(col("members_email") == 'robert.altmiller@databricks.com'))

# COMMAND ----------

# MAGIC %md # Document references
# MAGIC
# MAGIC - https://docs.databricks.com/en/dev-tools/python-sql-connector.html
# MAGIC - https://docs.databricks.com/api/account/accountgroups/delete
# MAGIC - https://databricks-sdk-py.readthedocs.io/en/latest/
# MAGIC - https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-groups.html
