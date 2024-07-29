# Databricks notebook source
# DBTITLE 1,Get Databricks Rest 2.0 Initial Configuration and Base Functions
# MAGIC %run "../general/base"

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Create Secret Scope
def create_secret_scope(dbricks_instance = None, dbricks_pat = None, scope_name = None):
  """create databricks secret scope"""
  jsondata = {"scope": scope_name}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, "secrets/scopes", "create"), dbricks_pat, jsondata)
  return response


# dbricks_scope_name = "raqo"
# response = create_secret_scope(databricks_instance, databricks_pat, dbricks_scope_name)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Delete Secret Scope
def delete_secret_scope(dbricks_instance = None, dbricks_pat = None, scope_name = None):
  """delete databricks secret scope"""
  jsondata = {"scope": scope_name}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, "secrets/scopes", "delete"), dbricks_pat, jsondata)
  return response


# dbricks_scope_name = "raqo"
# response = delete_secret_scope(databricks_instance, databricks_pat, dbricks_scope_name)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - List All Secret Scopes
def list_all_secret_scopes(dbricks_instance = None, dbricks_pat = None):
  """list all databricks secret scopes in a single workspace"""
  try:
    jsondata = None
    response = execute_rest_api_call(get_request, get_api_config(dbricks_instance, "secrets/scopes", "list"), dbricks_pat, jsondata)
    scopes = []
    for scope in json.loads(response.text)["scopes"]:
      scopes.append(scope["name"])
    return scopes
  except: return None
  

# scopes = list_all_secret_scopes(databricks_instance, databricks_pat)
# print(f"total_secret_scopes: {len(scopes)}; secret_scopes: {scopes}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Put Secret in Secret Scope
def put_secret_in_secret_scope(dbricks_instance = None, dbricks_pat = None, scope_name = None, secret_name = None, secret_value = None):
  """put a secret in a databricks secret scope"""
  jsondata = {
    "scope": scope_name,
    "key": secret_name,
    "string_value": secret_value
  }
  return execute_rest_api_call(post_request, get_api_config(dbricks_instance, "secrets", "put"), dbricks_pat, jsondata)


# dbricks_scope_name = "raqo"
# dbricks_secret_name = "region"
# dbricks_secret_value = "southcentralus"
# response = put_secret_in_secret_scope(databricks_instance, databricks_pat, dbricks_scope_name, dbricks_secret_name, dbricks_secret_value)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Delete Secret in Secret Scope
def delete_secret_in_secret_scope(dbricks_instance = None, dbricks_pat = None, scope_name = None, secret_name = None):
  """delete a secret in a databricks secret scope"""
  jsondata = {
    "scope": scope_name,
    "key": secret_name
  }
  return execute_rest_api_call(post_request, get_api_config(dbricks_instance, "secrets", "delete"), dbricks_pat, jsondata)


# dbricks_scope_name = "raqo"
# dbricks_secret_name = "test2"
# response = delete_secret_in_secret_scope(databricks_instance, databricks_pat, dbricks_scope_name, dbricks_secret_name)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - List All Secrets in a Secret Scope
def list_all_secret_scopes_secrets(dbricks_instance = None, dbricks_pat = None, scope_name = None):
  """list databricks secret scopes secrets"""
  try:
    jsondata = {"scope": scope_name}
    response = execute_rest_api_call(get_request, get_api_config(dbricks_instance, "secrets", "list"), dbricks_pat, jsondata)
    secrets = []
    for secret in json.loads(response.text)["secrets"]:
        secrets.append(secret["key"])
    return secrets
  except: return None


# dbricks_scope_name = "raqo"
# secrets = list_all_secret_scopes_secrets(databricks_instance, databricks_pat, dbricks_scope_name)
# print(f"secret_scopes_secrets: {secrets}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Apply Access Control List (ACL) Permission to Secret Scope
def add_secret_scope_acl(dbricks_instance = None, dbricks_pat = None, scope_name = None, principal = None, permission = None):
  """add access control list permission to databricks secret scope"""
  jsondata = {
    "scope": scope_name,
    "principal": principal,
    "permission": permission
  }
  return execute_rest_api_call(post_request, get_api_config(dbricks_instance, "secrets/acls", "put"), dbricks_pat, jsondata)


# dbricks_scope_name = "raqo"
# dbricks_principal = "users"
# dbricks_permission = "MANAGE" # WRITE or MANAGE
# response = add_secret_scope_acl(databricks_instance, databricks_pat, dbricks_scope_name, dbricks_principal, dbricks_permission)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - Remove Access Control List (ACL) Permission to Secret Scope
def remove_secret_scope_acl(dbricks_instance = None, dbricks_pat = None, scope_name = None, principal = None):
  """remove access control list permission databricks secret scope"""
  jsondata = {
    "scope": scope_name,
    "principal": principal,
  }
  return execute_rest_api_call(post_request, get_api_config(dbricks_instance, "secrets/acls", "delete"), dbricks_pat, jsondata)


# dbricks_scope_name = "raqo"
# dbricks_principal = "robert.altmiller@databricks.com"
# response = remove_secret_scope_acl(databricks_instance, databricks_pat, dbricks_scope_name, dbricks_principal)
# print(f"response: {response}; response_text: {response.text}")

# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 - List All Access Control List (ACL) Permissions Applied to Secret Scope
def list_secret_scope_acls(dbricks_instance = None, dbricks_pat = None, scope_name = None):
  """list access control lists"""
  try:
    jsondata = {"scope": scope_name}
    response = execute_rest_api_call(get_request, get_api_config(dbricks_instance, "secrets/acls", "list"), dbricks_pat, jsondata)
    return json.loads(response.text)["items"]
  except: return None


# dbricks_scope_name = "raqo"
# secret_scope_acls = list_secret_scope_acls(databricks_instance, databricks_pat, dbricks_scope_name)
# print(f"secret_scope_acls: {secret_scope_acls}")

# COMMAND ----------

# DBTITLE 1,Delete All Secret Scopes in Databricks Workspace
def delete_all_secret_scopes(dbricks_instance = None, dbricks_pat = None):
  """delete all secret scopes in a databricks workspace"""
  scopes = list_all_secret_scopes(databricks_instance, databricks_pat)
  for scope in scopes:
    response = delete_secret_scope(databricks_instance, databricks_pat, scope)
    print(f"{scope} deleted: {response}")

# COMMAND ----------

# DBTITLE 1,Create Workspace Secret Scope Report - Applies to a Single Secret Scope or to All Secret Scopes
def get_secret_scope_report(dbricks_instance = None, dbricks_pat = None, read_scope_user = None, read_scope_user_perms = None, secret_scope_name = None):
  """
  get a report of all the secret scopes, secret scope secrets, and permissions on secret scopes
  secret_scope_name parameter can be 'None' or the name of an actual secret scope
  if secret_scope_name is 'None' all secret scopes will be processed in the databricks workspace
  """
  
  SS_REPORT_ITEMS = {}
  SS_REPORT_FINAL = []

  # iterate over all workspace secret scopes and get secret scopes data
  if secret_scope_name == None: 
    # process all workspace secret scopes
    secret_scopes = list_all_secret_scopes(dbricks_instance, dbricks_pat)
  else: # process one single secret scope 
    secret_scopes = [secret_scope_name] 
  
  counter = 1
  for secret_scope in secret_scopes:
    # databricks instance / workspace name
    workspace = str(' '.join([x for x in get_api_config(dbricks_instance)["databricks_host"]]))
    SS_REPORT_ITEMS["workspace"] = workspace

    # print secret scope processing status
    print(f'{counter}. secret_scope "{secret_scope}" processed.....')

    # secret scope name
    SS_REPORT_ITEMS["secret_scope_name"] = secret_scope

    # get all access control list permissions for secret scope
    response_acl_perm_list = list_secret_scope_acls(dbricks_instance, dbricks_pat, secret_scope)
    SS_REPORT_ITEMS["secret_scope_acls"] = response_acl_perm_list

    # get all secrets in secret scope
    secret_names = list_all_secret_scopes_secrets(dbricks_instance, dbricks_pat, secret_scope)
    SS_REPORT_ITEMS["secret_scope_secret_names"] = secret_names

    # apply access control list (ACL) permission to group to be able to read secret values
    response_acl_applied = add_secret_scope_acl(dbricks_instance, dbricks_pat, secret_scope, read_scope_user, read_scope_user_perms)

    # read all secret scope secret values for secret scope report
    if secret_names != None:
      secret_vals = {}
      for secret_name in secret_names:
        secret_value = ' '.join([x for x in dbutils.secrets.get(scope = secret_scope, key = secret_name)])
        secret_vals.update({secret_name: secret_value})
      SS_REPORT_ITEMS["secret_scope_secrets"] = secret_vals
    else: SS_REPORT_ITEMS["secret_scope_secrets"] = secret_names
    
    # remove access control list (ACL) permission to group to restore original secret scope acls
    response_acl_removed = remove_secret_scope_acl(dbricks_instance, dbricks_pat, secret_scope, read_scope_user)

    SS_REPORT_FINAL.append(SS_REPORT_ITEMS)
    SS_REPORT_ITEMS = {}
    counter += 1
  return json.dumps(SS_REPORT_FINAL)

# COMMAND ----------

# DBTITLE 1,Execute Workspace Secret Scope Report For Recreation of a Single Secret Scope or All Workspace Secret Scopes
def recreate_all_secret_scopes(dbricks_instance = None, dbricks_pat = None, instructions = None, write_scope_user = None, write_scope_user_perms = None,  new_secret_scope_name = None):
  """
  recreates all secret scopes in a databricks workspace with correct permissions and secrets all copied over
  new_secret_scope can be 'None' or the name of a new secret scope.  If 'None' overwrite the same secret scope, and
  if new_secret_scope != None then create a new secret scope, apply permissions, and copy secrets
  """
  
  json_secret_scope_obj = json.loads(instructions)

  for secretscope in json_secret_scope_obj:

    # workspace name
    workspace_name = secretscope['workspace'].replace(' ', '') #redacted
    
    if new_secret_scope_name == None: secret_scope_name = secretscope['secret_scope_name'] # overwrite same secret scope 
    else: secret_scope_name = new_secret_scope_name # make a new secret scope
    
    # secret scope acls and secrets lists
    secret_scope_acls = secretscope["secret_scope_acls"] # secret scope acls list
    secret_scope_secrets = secretscope["secret_scope_secrets"] # secret scope secrets list

    # delete secret scope
    print(f'delete secret scope "{secret_scope_name}": \
      {delete_secret_scope(dbricks_instance, dbricks_pat, secret_scope_name)}')
    
    # create secret scope
    print(f'create secret scope "{secret_scope_name}": \
      {create_secret_scope(dbricks_instance, dbricks_pat, secret_scope_name)}')

    # apply access control list (ACL) permission to group to write secret values
    response_acl_applied = add_secret_scope_acl(dbricks_instance, dbricks_pat, secret_scope_name, write_scope_user, write_scope_user_perms)

    # add acl permissions to secret scope
    if secret_scope_acls != None:
      for acl in secret_scope_acls:
        principal = acl["principal"]
        permission = acl["permission"]
        print(f'apply permission {acl} to secret scope "{secret_scope_name}": \
            {add_secret_scope_acl(dbricks_instance, dbricks_pat, secret_scope_name, principal, permission)}')
    else: print("no secret scope acls to add....")

    # add secrets to secret scope
    if secret_scope_secrets != None:
      for key, val in secret_scope_secrets.items():
        secret_name = key
        secret_value = val.replace(' ', '') # redacted
        print(f'add secret {secret_name} and value "{secret_value}" to secret scope "{secret_scope_name}": \
          {put_secret_in_secret_scope(dbricks_instance, dbricks_pat, secret_scope_name, secret_name, secret_value)}')
    else: print("no secret scope secrets to add....")
    
    # remove access control list (ACL) permission to group to restore original secret scope acls
    response_acl_removed = remove_secret_scope_acl(dbricks_instance, dbricks_pat, secret_scope_name, write_scope_user)

    # break after one loop because we created a new secret scope based on the settings in 'instructions'
    if new_secret_scope_name != None: break
    print("\n")
