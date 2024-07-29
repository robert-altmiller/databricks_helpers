# Databricks notebook source
# DBTITLE 1,Get Databricks Rest 2.0 Initial Configuration and Base Functions
# MAGIC %run "./secret_scope_base"

# COMMAND ----------

# DBTITLE 1,Notebook Parameters Initialization
# delete secret scope report from azure storage account (True or False)
delete_ss_report_from_azsa = False

# delete secret scope report from dbfs (True or False)
delete_ss_report_from_dbfs = True

# recreate all secret scopes (True or False)
recreate_all_secret_scopes = True

# secret scopes to re-create
recreate_secret_scopes_list = ["adnan"] # user defined (default = [])
if len(recreate_secret_scopes_list) > 0: recreate_all_secret_scopes = False

# COMMAND ----------

# DBTITLE 1,Get Deploy Secret Scopes Instructions From Old Workspace
# user defined parameters
storage_account_obj.set_azure_storage_acct_container_name_override("dbricks-secret-scope")
storage_account_obj.set_azure_storage_acct_subfolder_path_override("secret_scope")
storage_account_obj.set_azure_storage_acct_file_name_override("secret_scope.json")

# download secret scope json report locally in new workspace
dbfsfilepath = storage_account_obj.download_blob_write_locally(
    storageacctname = storage_account_obj.config["AZURE_STORAGE_ACCOUNT_NAME"],
    container = storage_account_obj.config["AZURE_STORAGE_ACCOUNT_CONTAINER"],
    folderpath = storage_account_obj.config["AZURE_STORAGE_ACCOUNT_FOLDER_PATH"],
    filename = storage_account_obj.config["AZURE_STORAGE_ACCOUNT_FILE_NAME"]
)

# get secret scope deploy instructions for new workspace
with open(dbfsfilepath) as fp:
    data = json.load(fp)
deploy_instructions = data["payload"]
if deploy_instructions != None:
  deploy_instructions_json = json.loads(deploy_instructions)

if delete_ss_report_from_dbfs == True:
  # remove local copied secret scope folder in dbfs
  shutil.rmtree(f'./{storage_account_obj.config["LOCAL_DATA_FOLDER"]}', ignore_errors = True)
  print(f'./{storage_account_obj.config["LOCAL_DATA_FOLDER"]} removed successfully from dbfs....')

if delete_ss_report_from_azsa == True:
  # remove secret scope storage account container
  storage_account_obj.delete_container(storage_account_obj.config["AZURE_STORAGE_ACCOUNT_CONTAINER"])
  print(f'{storage_account_obj.config["AZURE_STORAGE_ACCOUNT_CONTAINER"]} container removed successfully....')

# COMMAND ----------

# DBTITLE 1,Print All Secret Scopes for Migration to New Workspace
allscopes = []
for jsonrecord in deploy_instructions_json:
    allscopes.append(jsonrecord["secret_scope_name"])
print(allscopes)

# COMMAND ----------

# DBTITLE 1,Get Final Instructions to Create Single Secret Scope or All Secret Scopes
if recreate_all_secret_scopes == True:
  deploy_instructions_final = deploy_instructions
else: # get subset of secret scopes based on deploy_instructions
  scopes_subset = []
  for scope in recreate_secret_scopes_list:
    for jsonrecord in deploy_instructions_json:
      if jsonrecord["secret_scope_name"] == scope:
        scopes_subset.append(jsonrecord)
  deploy_instructions_final = json.dumps(scopes_subset)
print(deploy_instructions_final)

# COMMAND ----------

# DBTITLE 1,Run Secret Scope Creation Final Deployment Instructions From Previous Step For New Workspace
# recreate secret scopes in new workspace (works across cloud environments too)
recreate_all_secret_scopes(databricks_migration_instance, databricks_migration_pat, deploy_instructions_final, write_scope_user = "robert.altmiller@databricks.com", write_scope_user_perms = "Write", new_secret_scope_name = "adnan")

# COMMAND ----------

# DBTITLE 1,Validate Secrets Scopes Migrated to New Workspace Successfully
# if 'secret_scope_name' is 'None' then it will process all workspace secret scopes
secret_scope_instructions = get_secret_scope_report(databricks_migration_instance, databricks_migration_pat, read_scope_user = "robert.altmiller@databricks.com", read_scope_user_perms = "READ", secret_scope_name = None)
print(secret_scope_instructions)
