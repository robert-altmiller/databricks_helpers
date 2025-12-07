# Databricks notebook source
# DBTITLE 1,Local Parameters
try: # Define iam catalog
    catalog = dbutils.widgets.get("iam_catalog")
except:
    catalog = "datamesh_sandbox_roadshow_demo_dev_azr_westus"

try: # Define iam schema
    schema = dbutils.widgets.get("iam_schema")
except:
    schema = "datamesh_iam_schema"

try: # Define system tables
    system_tables = [dbutils.widgets.get("system_table")]
except:
    # All these system tables have a workspace column defined
    system_tables = [
    "system.access.assistant_events", "system.access.audit", "system.access.column_lineage", 
    "system.access.outbound_network", "system.access.table_lineage", "system.access.workspaces_latest",
    "system.compute.clusters", "system.compute.node_timeline", "system.compute.warehouse_events",
    "system.compute.warehouses", "system.lakeflow.job_run_timeline", "system.lakeflow.job_task_run_timeline",
    "system.lakeflow.job_tasks", "system.lakeflow.jobs", "system.query.history",
    "system.serving.served_entities", "system.storage.predictive_optimization_operations_history",
    "system.billing.usage"
    ]

# COMMAND ----------

# DBTITLE 1,Define Filtering Common Table Expression (Admins)
ADMIN_GROUPS_USERS_CTE_NAME = "admin_groups_users"

SQL_ROW_FILTER_CTE = f"""
    WITH {ADMIN_GROUPS_USERS_CTE_NAME} AS (
    SELECT DISTINCT workspace_id, workspace_url
    FROM 
        {catalog}.{schema}.all_workspace_and_catalog_perms_admins
    WHERE 
        (admins.group_name IS NOT NULL OR admins.user_name IS NOT NULL) AND
        (is_account_group_member(ADMINS.group_name) = true OR current_user() = ADMINS.user_name)
    )
"""

# COMMAND ----------

# DBTITLE 1,Create System Table Filtered Dynamic Views
# Create system table row filtered dynamic views (e.g. workspace level)
for table in system_tables:
  system_vw_name = f"vw_{table.replace('.','_')}"
  try:
    spark.sql(f"""
      CREATE OR REPLACE VIEW {catalog}.{schema}.{system_vw_name} AS
      {SQL_ROW_FILTER_CTE}
      SELECT x.*, ADMINS.workspace_url AS workspace_url
      FROM 
      {table} x
      JOIN 
      {ADMIN_GROUPS_USERS_CTE_NAME} ADMINS ON x.workspace_id = ADMINS.workspace_id
    """)
    print(f"finished creating system table view {system_vw_name}")
  except:
    spark.sql(f"""
      CREATE OR REPLACE VIEW {catalog}.{schema}.{system_vw_name} AS
      {SQL_ROW_FILTER_CTE}
      SELECT x.*
      FROM 
      {table} x
      JOIN 
      {ADMIN_GROUPS_USERS_CTE_NAME} ADMINS ON x.workspace_id = ADMINS.workspace_id
    """)
    print(f"finished creating system table view {system_vw_name}")
  except Exception as e:
    print(f"unable to create view {system_vw_name}: {e}")

# COMMAND ----------

# DBTITLE 1,Unit Test (vw_system_access_audit)
for table in system_tables:
  system_vw_name = f"vw_{table.replace('.','_')}"
  X = spark.sql(f"SELECT COUNT(*) FROM {table}").first()[0]
  print(f"{table} count: {X}")
  Y = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}.{system_vw_name}").first()[0]
  print(f"{system_vw_name} count: {Y}\n")