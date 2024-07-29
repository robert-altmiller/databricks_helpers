# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install --quiet databricks-sql-connector

# COMMAND ----------

# DBTITLE 1,Restart Python Interpreter for Python SQL Connector
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
import os, requests, json
from pyspark.sql.functions import explode, col, lit
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

# COMMAND ----------

# DBTITLE 1,User Defined Parameters
# connection parameters
DATABRICKS_HOST = "https://accounts.cloud.databricks.com"
ACCOUNT_ID = "0d26daa6-5e44-4c97-a497-ef015f91254a"
CLIENT_ID = ""
CLIENT_SECRET = ""

# sql connect parameters
sql_account_id = "e6e8162c-a42f-43a0-af86-312058795a14"
sql_client_id = ""
sql_client_secret = ""
sql_server_hostname = "https://e2-demo-field-eng.cloud.databricks.com"
sql_http_path = "/sql/1.0/warehouses/475b94ddc7cd5211"

# COMMAND ----------

# DBTITLE 1,SQL - Display of User Group Members at Workspace Level
# enter a user name
user_name = "robert.altmiller@databricks.com"

results = spark.sql(f"""SHOW GROUPS WITH USER `{user_name}`""") \
  .withColumn("userName", lit(user_name)) \
  .where(col("directGroup") == True)
display(results)

# COMMAND ----------

# DBTITLE 1,Python Interface to SQL Rest API (User Group Members at Workspace Level)
def credential_provider(host_name, client_id, client_secret):
  """sql connect credential provider"""
  config = Config(
    host          = host_name,
    client_id     = client_id,
    client_secret = client_secret
  )
  return oauth_service_principal(config)


def query_data(SQL, host_name, http_path, client_id, client_secret):
  """query data with sql connect"""

  # Ensure the credentials_provider function is defined and properly imports necessary modules
  conn = sql.connect(
    server_hostname = host_name,
    http_path = http_path,
    credentials_provider = lambda: credential_provider(host_name, client_id, client_secret)
  )

  result = None
  try:
    cursor = conn.cursor()
    cursor.execute(SQL)
    result = cursor.fetchall()
  except Exception as e:
    print(f"An error occurred: {e}")
  finally:
    conn.close()
  return result


# enter user name
usernames = ["robert.altmiller@databricks.com", "douglas.moore@databricks.com"]
# sql command to execute using sql connect

result_list = []
for user in usernames:
  SQL = f"""SHOW GROUPS WITH USER `{user}`"""
  result = query_data(
    SQL = SQL, 
    host_name = sql_server_hostname, 
    http_path = sql_http_path, 
    client_id = sql_client_id, 
    client_secret = sql_client_secret
  )
  result_dict = {'userName': user, 'groups': [{'groupName': row.name, 'directGroup': row.directGroup} for row in result]}
  result_list.append(result_dict)

result_json = json.dumps(result_list, indent = 2)
print(result_json)

# COMMAND ----------

# MAGIC %md # Document references
# MAGIC
# MAGIC - https://docs.databricks.com/en/dev-tools/python-sql-connector.html
# MAGIC - https://docs.databricks.com/api/account/accountgroups/delete
# MAGIC - https://databricks-sdk-py.readthedocs.io/en/latest/
# MAGIC - https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-groups.html
