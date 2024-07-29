# Databricks notebook source
# DBTITLE 1,Get Databricks Rest 2.0 Initial Configuration and Base Functions
# MAGIC %run "../general/base"

# COMMAND ----------

# MAGIC %run "../databricks_secret_scope/secret_scope"

# COMMAND ----------

# DBTITLE 1,Create Secret Scope and Add Service Principle App Id and Secret


# COMMAND ----------

# databricks secret scope names

# service principle
client_id = dbutils.secrets.get(scope = "client-id",key="<service-credential-key-name>")
# service principle password
client_secret = dbutils.secrets.get(scope="client-secret",key="<service-credential-key-name>")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)
