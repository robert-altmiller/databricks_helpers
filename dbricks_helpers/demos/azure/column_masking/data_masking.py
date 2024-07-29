# Databricks notebook source
# DBTITLE 1,Shared Notebook Imports
# MAGIC %run "./import_data_from_excel"

# COMMAND ----------

# DBTITLE 1,Local Data Masking Parameters (User Defined)
# specify schema in which masked views are to be created and choose a suffix for the view name
views_schema = "tlp_view"
views_prefix = "v_"

# COMMAND ----------

# DBTITLE 1,Build Reusable functions
def get_rules(audit_df):
    """
    This function reads the audit metadata table and creates full_table_name and full_column_name
    columns and caches the dataframe as it is use multiple times in the process
    """
    df = audit_df \
      .filter("active_ind = 'Y'") \
      .withColumn("full_table_name", F.expr("'`' || catalog_name || '`.`' || schema_name || '`.`' || table_name || '`'")
      ) \
      .withColumn("full_column_name", F.expr("full_table_name || '.`' || column_names || '`'"))
    return df


def mask_column(column_name, mask_type, groups_allowed, data_type):
    """
    This function generates the code to mask the column in the SQL code. It supports below masking types:
      1. Static           - 8 asterisks symbols are hardcoded
      2. Alpha            - Replaces all alphabets from the column with asterisk
      3. Numeric          - Replaces all numbers from the column with asterisk
      4. AlphaNumeric     - Replaces all alphabets and numbers from the column with asterisk
    If no groups are specified, column is masked as Static and masked for all users
    """
    masked_column = f"'********' as {column_name}"
    if groups_allowed is None or mask_type is None:
        return masked_column
    mast_type = mask_type.lower()
    groups = " OR ".join(
        ["is_member('{}')".format(group) for group in groups_allowed.split(",")]
    )
    if data_type.lower() != "string":
        column_string = f"CAST({column_name} AS STRING)"
    else:
        column_string = column_name
    if mask_type == "static":
        masked_column = "'********'"
    elif mask_type == "alpha":
        masked_column = f"regexp_replace({column_string},'[a-zA-Z]' ,'*' )"
    elif mask_type == "numeric":
        masked_column = f"regexp_replace({column_string},'[0-9]' ,'*' )"
    elif mask_type == "alphanumeric":
        masked_column = f"regexp_replace({column_string},'[0-9a-zA-Z]' ,'*' )"
    case_statement = f"CASE WHEN {groups} THEN {column_name} ELSE {masked_column} END as {column_name}"
    return case_statement

spark.udf.register("mask_column", mask_column)

# COMMAND ----------

# DBTITLE 1,Read Metadata Table to get Masking Rules
# read in the masking metadata (e.g. audit table) for masking rules
# audit_catalog, audit_schema, and audit_table are defined in 'import_data_from_excel' notebook
masking_metadata = spark.sql(f"SELECT * FROM `{audit_catalog}`.`{audit_schema}`.`{audit_table}`")
# Find the maximum value of the 'last_updated' column
max_last_updated = masking_metadata.agg({"last_updated": "max"}).collect()[0][0]
# Filter rows where 'last updated' is equal to the maximum value
masking_metadata = masking_metadata.filter(masking_metadata["last_updated"] == max_last_updated)

# get masking rules
rules = get_rules(masking_metadata)

# create list of all tables where masking will be applied
unique_tables = rules.select(
    "catalog_name", "schema_name", "table_name", "full_table_name"
).distinct()

# create list of all columns where masking will be applied
unique_columns = rules.select("full_column_name").distinct()
display(rules)

# COMMAND ----------

# DBTITLE 1,Get List of Tables For Which Metadata Needs to be Pulled From Information Schema
# Three python lists are created (catalogs, schemas, tables) for which table structures must be extracted from information schema. 
# This step is used to filter down the data from information schema for performance reasons

list_of_objects_for_metadata = unique_tables.select(
    F.expr("collect_set(catalog_name)").alias("catalogs"),
    F.expr("collect_set(schema_name)").alias("schemas"),
    F.expr("collect_set(table_name)").alias("tables"),
).collect()
required_catalog_list = "','".join(list_of_objects_for_metadata[0]["catalogs"])
required_schema_list = "','".join(list_of_objects_for_metadata[0]["schemas"])
required_table_list = "','".join(list_of_objects_for_metadata[0]["tables"])

# COMMAND ----------

# DBTITLE 1,Get All Table Metadata From the Information Schema
columns_metadata = spark.read.table("system.information_schema.columns") \
  .filter("table_catalog IN ('{}') and table_schema IN ('{}') and table_name IN ('{}')".format(required_catalog_list,  
  required_schema_list, required_table_list)) \
  .select("table_catalog","table_schema","table_name","column_name","ordinal_position","data_type") \
  .withColumn("full_table_name", F.expr("'`' || table_catalog || '`.`' || table_schema || '`.`' || table_name || '`'")) \
  .withColumn("full_column_name", F.expr("full_table_name || '.`' || column_name || '`'"))

display(columns_metadata)

# COMMAND ----------

# DBTITLE 1,Filter Down to Required Tables and Indicate Columns That Need Masking
columns_metadata_enriched = columns_metadata.join(unique_tables, ["full_table_name"], "left_semi") \
  .join(rules.select("full_column_name", "masking_type", "groups_allowed"), ["full_column_name"], "left") \
  .orderBy("full_table_name", "ordinal_position")

display(columns_metadata_enriched)

# COMMAND ----------

# DBTITLE 1,Generate SQL DDLs for View Creation with Column Masking
tables_list = unique_tables.collect()

ddls = []
for table in tables_list:
  
  l_catalog = table.catalog_name
  l_schema = table.schema_name
  l_table = table.table_name
  l_full_table_name = table.full_table_name

  l_full_view_name = f"{l_catalog}.{views_schema}.{views_prefix}{l_table}"
  table_columns = columns_metadata_enriched \
    .filter("full_table_name = '{}'".format(l_full_table_name)) \
    .selectExpr("*", "CASE WHEN masking_type IS NULL THEN column_name ELSE mask_column(column_name, masking_type, groups_allowed, data_type) END AS masked_column") \
    .orderBy("ordinal_position") \
    .selectExpr("collect_list(masked_column) as columns").collect()
    
  table_columns_formatted = ",\n".join(table_columns[0]['columns'])
  ddl = f"CREATE OR REPLACE VIEW {l_full_view_name} \nAS \nSELECT \n{table_columns_formatted} \nFROM \n{l_full_table_name};"
  ddls.append(ddl)

# COMMAND ----------

# DBTITLE 1,Print the Data Definition Language (DDLs)
for ddl in ddls:
  print(ddl)

# COMMAND ----------

# DBTITLE 1,Execute Data Definition Language (DDLs) List in Spark SQL
# Final step executes each of the view DDLs one by one and creates the views based on the rules from metadata table
for ddl in ddls:
  spark.sql(ddl)
