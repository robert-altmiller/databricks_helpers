# Databricks notebook source
# DBTITLE 1,Get Databricks Rest 2.0 Initial Configuration and Base Functions
# MAGIC %run "./groups_base"

# COMMAND ----------

# DBTITLE 1,Notebook Parameters Initialization
# delete secret scope report from azure storage account (True or False)
delete_groups_report_from_azsa = False

# delete secret scope report from dbfs (True or False)
delete_groups_report_from_dbfs = True

# recreate all secret scopes (True or False)
recreate_all_groups = True

# secret scopes to re-create
recreate_groups_list = ["test123"] # user defined (e.g. default = [])
if len(recreate_groups_list) > 0: recreate_all_groups = False

# COMMAND ----------

# DBTITLE 1,Get Deploy Groups Instructions From Old Workspace
# user defined parameters
storage_account_obj.set_azure_storage_acct_container_name_override("dbricks-groups")
storage_account_obj.set_azure_storage_acct_subfolder_path_override("groups")
storage_account_obj.set_azure_storage_acct_file_name_override("groups.json")


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

if delete_groups_report_from_dbfs == True:
  # remove local copied secret scope folder in dbfs
  shutil.rmtree(f'./{storage_account_obj.config["LOCAL_DATA_FOLDER"]}', ignore_errors = True)
  print(f'./{storage_account_obj.config["LOCAL_DATA_FOLDER"]} removed successfully from dbfs....')

if delete_groups_report_from_azsa == True:
  # remove secret scope storage account container
  storage_account_obj.delete_container(storage_account_obj.config["AZURE_STORAGE_ACCOUNT_CONTAINER"])
  print(f'{storage_account_obj.config["AZURE_STORAGE_ACCOUNT_CONTAINER"]} container removed successfully....')

# COMMAND ----------

# DBTITLE 1,Print All Groups for Migration to New Workspace
allgroups = []
for jsonrecord in deploy_instructions_json:
    allgroups.append(jsonrecord["group_name"])
print(allgroups)

# COMMAND ----------

# DBTITLE 1,Get Final Instructions to Create Single Group or All Groups
if recreate_all_groups == True:
  deploy_instructions_final = deploy_instructions
else: # get subset of secret scopes based on deploy_instructions
  groups_subset = []
  for group in recreate_groups_list:
    for jsonrecord in deploy_instructions_json:
      if jsonrecord["group_name"] == group:
        groups_subset.append(jsonrecord)
  deploy_instructions_final = json.dumps(groups_subset)
print(deploy_instructions_final)

# COMMAND ----------

# DBTITLE 1,Run Groups Creation Final Deployment Instructions From Previous Step For New Workspace
# recreate groups in new workspace (works across cloud environments too)
recreate_all_groups(databricks_migration_instance, databricks_migration_pat, deploy_instructions_final,  new_group_name = None)

# COMMAND ----------

# DBTITLE 1,Validate Groups Migrated to New Workspace Successfully
# if 'group_name' is 'None' then it will process all workspace groups
group_instructions = get_groups_report(databricks_instance, databricks_pat, group_name = None)
print(group_instructions)
