# Databricks notebook source
# DBTITLE 1,Get Databricks Rest 2.0 Initial Configuration and Base Functions (Secret Scopes)
# MAGIC %run "./secret_scope_base"

# COMMAND ----------

# DBTITLE 1,Read All Secret Scopes or Single Secret Scope in Workspace
# if 'secret_scope_name' is 'None' then it will process all workspace secret scopes
secret_scope_instructions = get_secret_scope_report(databricks_instance, databricks_pat, read_scope_user = "robert.altmiller@databricks.com", read_scope_user_perms = "READ", secret_scope_name = None)

# COMMAND ----------

# DBTITLE 1,Write Secret Scope Instructions to DBFS (Local) and Azure Storage Account (External)
# user defined parameters
storage_account_obj.set_azure_storage_acct_container_name_override("dbricks-secret-scope")
storage_account_obj.set_azure_storage_acct_subfolder_path_override("secret_scope")
storage_account_obj.set_azure_storage_acct_file_name_override("secret_scope.json")

# write out groups to azure storage account
upload_to_dbfs_and_azure_storage(storage_account_obj, secret_scope_instructions)
