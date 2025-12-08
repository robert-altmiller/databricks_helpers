# Databricks notebook source
# DBTITLE 1,Library Imports
import os
from typing import List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Row, DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Setup Databricks Widgets
dbutils.widgets.text("Catalog", "", "Enter Catalog Name (Mandatory):")
dbutils.widgets.text("Schema", "", "Enter Schema Name (Optional):")
dbutils.widgets.text("Table", "", "Enter Table Name (Optional):")
dbutils.widgets.text("Output Path", "", "Enter Base Volumes Path (Mandatory):")
dbutils.widgets.text("Model Serving Endpoint Name", "databricks-meta-llama-3-3-70b-instruct", "Model Serving Endpoint Name (Mandatory):")
dbutils.widgets.text("Sample Data Limit", "2", "Sample Data Limit (Mandatory):")
dbutils.widgets.text("Sample Max Cell Chars", "1000", "Sample Max Cell Chars (Mandatory):")
dbutils.widgets.dropdown("Always Update Comments", choices=["true", "false"], defaultValue="true", label="Always Update Comments (Optional):")
dbutils.widgets.dropdown("Prompt Return Length", choices=["100", "150", "200", "250", "300", "350", "400", "450", "500", "550", "600"], defaultValue="200", label="Prompt Return Length (Mandatory):")

# COMMAND ----------

# DBTITLE 1,Local Parameters
# UC table name (mandatory)
catalog = dbutils.widgets.get("Catalog")
print(f"catalog: {catalog}")

# UC schema name (optional)
schema = dbutils.widgets.get("Schema")
print(f"schema: {schema}")

# UC table name (optional)
table = dbutils.widgets.get("Table")
print(f"table: {table}")

# UC Volumes path to store the output table description results
output_path = dbutils.widgets.get("Output Path") + "/bulk_comments/tables"
print(f"output_path: {output_path}")

# Databricks LLM model serving endpoint
endpoint_name = dbutils.widgets.get("Model Serving Endpoint Name")
print(f"endpoint_name: {endpoint_name}")

# Sample Data Limit specifies how many rows of data to return per table
data_limit = int(dbutils.widgets.get("Sample Data Limit"))
print(f"data_limit: {data_limit}")

# Sample Max Cell Chars specifies how many character to return per row-col.
max_cell_chars = int(dbutils.widgets.get("Sample Max Cell Chars"))
print(f"max_cell_chars: {max_cell_chars}")

# Prompt return table description length
prompt_return_length = int(dbutils.widgets.get("Prompt Return Length"))
print(f"prompt_return_length: {prompt_return_length}")

# Overwrite AI-generated table descriptions
always_update = dbutils.widgets.get("Always Update Comments").lower() == "true"
print(f"always_update: {always_update}")

# Output file format (e.g., csv or json)
output_file_format = "json" # or csv
print(f"output_file_format: {output_file_format}")

# Calculate default parallelism
try: default_parallelism = spark.sparkContext.defaultParallelism / 2 # Does not work with serverless
except: default_parallelism = (4 * os.cpu_count()) - 1
print(f"Number of executors: {default_parallelism}")

# COMMAND ----------

# DBTITLE 1,Get Table Metadata
def get_table_metadata(catalog: str, schema: str, table: str, data_limit: int = 1, max_cell_chars = 1000) -> dict:
    """
    Fetch schema information, row count, and sample rows for a given table.
    Truncates only *individual column values* that exceed a safe character limit to prevent
    overly long AI prompts (e.g., large array or JSON string columns).
    Args:
        catalog (str): The catalog name.
        schema (str): The schema/database name.
        table (str): The table name.
        data_limit (int, optional): Number of sample rows to retrieve. Default = 1.
        max_cell_chars (int, optional): Maximum number of characters per cell value. Default = 1000.
    Returns:
        dict: Dictionary containing:
            - catalog (str): Catalog name
            - schema (str): Schema name
            - table (str): Table name
            - schema_str (str): Comma-separated "col_name col_type" string
            - samples (list[dict]): List of sample rows (as Python dicts),
              where long individual cell values are safely truncated.
    """
    # Load the table as a Spark DataFrame
    df = spark.table(f"{catalog}.{schema}.{table}")
    # Build a simple string representation of the table schema
    schema_str = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields])

    # ------------------------------------------------------------------
    # Safety configuration: 'max_cell_chars' parameter limits the number of 
    # characters per *cell value* in sample data. This avoids large array or 
    # string columns blowing up the AI prompt size.
    # ------------------------------------------------------------------

    # Initialize list for storing sample rows
    samples = []

    # Collect up to `data_limit` rows from the table
    for row in df.limit(data_limit).collect():
        rdict = {}

        # Iterate through each column in the row
        for col, val in row.asDict(recursive=True).items():
            if val is None:
                # Preserve null values as-is
                rdict[col] = None
            else:
                val_str = str(val)

                # Truncate only if individual value exceeds the configured limit
                if len(val_str) > max_cell_chars:
                    rdict[col] = val_str[:max_cell_chars] + " ...[truncated]"
                else:
                    rdict[col] = val_str

        # Append the sanitized row to the sample list
        samples.append(rdict)

    # Return the metadata dictionary for this table
    return {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "schema_str": schema_str,
        "samples": samples,
    }


# Get table metadata with a few sample rows based on data_limit and max cell chars.
table_metadata = get_table_metadata(catalog, schema, table, data_limit = data_limit, max_cell_chars = max_cell_chars)
print(table_metadata)

# COMMAND ----------

# DBTITLE 1,Build Table Dynamic Table Prompt
def build_table_prompt(meta: dict, prompt_return_length: int) -> str:
    """
    Build an AI prompt for describing a table using schema and sample data.
    Args:
        meta (dict): Dictionary returned from get_table_metadata containing:
            - catalog (str)
            - schema (str)
            - table (str)
            - schema_str (str)
            - row_count (int)
            - samples (list[dict])
        prompt_return_length (int): Maximum number of words in the AI-generated description.
    Returns:
        str: A string prompt suitable for passing to ai_query.
    """
    sample_preview = str(meta["samples"])
    prompt = (

        f'Provide the business context / definition, related business processes, and how to use / business enablement for the Table Metadata below using the format below:\n\n'
        
        f'**BUSINESS CONTEXT - DEFINITION**:\n'
        f'[Example Format: - This data provides information about customer choices for in-season products, including details about product distribution, allocation, and lifecycle. It helps businesses understand how products are being distributed, allocated, and managed throughout their lifecycle, enabling informed decisions about product offerings, inventory management, and customer satisfaction.]\n'
        f'**RELATED BUSINESS PROCESSES**:\n'
        f'[Example Format: - Product Distribution and Allocation: This data supports the process of distributing products to various channels and allocating them to specific customer groups.]\n'
        f'**HOW TO USE - BUSINESS ENABLEMENT**:\n'
        f'[Example Format: - Use this data to analyze product distribution patterns and identify areas for improvement in allocation and inventory management..]\n'

        f'\nStrict Requirements:\n\n'
        f'- Use concise business language suitable for non-technical business users and avoid technical jargon in all the descriptions above.\n'
        f'- Do not use catalog names, schema names, table names, or data types in any of the descriptions above.\n'
        f'- The entire output should be <= {prompt_return_length} words in all the descriptions above.\n'

        f'\nTable Metadata:\n\n'
        f'- The table "{meta["table"]}" is in schema "{meta["schema"]}" within catalog "{meta["catalog"]}".\n'
        f'- The table columns are {meta["schema_str"]}.\n'
        f'- The sample data is: {sample_preview}.\n'
    )
    return prompt


# Get the table metadata prompt for AI-Query
prompt = build_table_prompt(table_metadata, prompt_return_length)
print(prompt)

# COMMAND ----------

# DBTITLE 1,Build Table Dynamic Table Prompt (Original)
# def build_table_prompt(meta: dict, prompt_return_length: int) -> str:
#     """
#     Build an AI prompt for describing a table using schema and sample data.
#     Args:
#         meta (dict): Dictionary returned from get_table_metadata containing:
#             - catalog (str)
#             - schema (str)
#             - table (str)
#             - schema_str (str)
#             - row_count (int)
#             - samples (list[dict])
#         prompt_return_length (int): Maximum number of words in the AI-generated description.
#     Returns:
#         str: A string prompt suitable for passing to ai_query.
#     """
#     sample_preview = str(meta["samples"])
#     prompt = (
#         f'This description will be stored as a Unity Catalog table description. '
#         f'Write a clear, concise, single-paragraph summary not exceeding {prompt_return_length} words. '
#         f'Describe the table "{meta["table"]}" in schema "{meta["schema"]}" within catalog "{meta["catalog"]}". '
#         f'The schema is: {meta["schema_str"]}. '
#         f'Here are some sample rows: {sample_preview}. '
#         f'Explain what kind of information this table contains and how it might be used for analysis. '
#         f'Avoid repeating schema or catalog names in the output.'
#     )
#     return prompt


# # # Get the table metadata prompt for AI-Query
# # # prompt = build_table_prompt(table_metadata, prompt_return_length)
# # # print(prompt)

# COMMAND ----------

# DBTITLE 1,Get Table Descriptions
def get_table_description_ai(table_metadata: dict, endpoint_name: str, prompt_return_length: int) -> str:
    """
    Generate a table description using ai_query given metadata.
    Args:
        table_metadata (dict): Table metadata dictionary from get_table_metadata.
        endpoint_name (str): The registered AI endpoint name used with ai_query.
        prompt_return_length (int): Maximum number of words in the AI-generated description.
    Returns:
        str: AI-generated description of the table.
    """
    prompt = build_table_prompt(table_metadata, prompt_return_length)
    query = f"SELECT ai_query('{endpoint_name}', :prompt) AS description"
    return spark.sql(query, args={"prompt": prompt}).collect()[0].description


# Get the table metadata description using prompt with AI-Query
# table_description = get_table_description_ai(table_metadata, endpoint_name, prompt_return_length)
# print(table_description)

# COMMAND ----------

# DBTITLE 1,Get Table Descriptions Using AI Query
def get_table_descriptions(catalog: str, schema: str = None, table: str = None, data_limit: int = 1, 
                           endpoint_name: str = None, max_workers: int = 8, replace_comment: bool = False, 
                           prompt_return_length: int = 200, max_cell_chars: int = 1000
    ) -> DataFrame:
    """
    Generate AI-assisted descriptions for tables and return them as a Spark DataFrame.
    This function:
      1. Fetches table metadata (catalog, schema, table name, existing comments).
      2. Retrieves schema + sample data via `get_table_metadata`.
      3. Calls AI (via `get_table_description_ai`) to generate new descriptions.
      4. Returns a Spark DataFrame with both existing and AI-generated comments.
    Args:
        catalog (str): Catalog name (required).
        schema (str, optional): Schema name. If None, process all schemas.
        table (str, optional): Table name. If None, process all tables in scope.
        data_limit (int, optional): Number of sample rows to include in AI prompt. Default = 1.
        endpoint_name (str, optional): AI endpoint name registered in Databricks. Required.
        max_workers (int, optional): Number of parallel workers for AI calls. Default = 8.
        replace_comment (bool, optional): Replace existing comment with AI-generated description. Default = False.
        prompt_return_length (int): Maximum number of words in the AI-generated description.
        max_cell_chars (int): Maximum number of characters in the schema string. Default = 1000.
    Returns:
        DataFrame: Spark DataFrame with schema:
            - table_catalog (str)
            - table_schema (str)
            - table_name (str)
            - replace_comment (bool) : True if table has no existing comment
            - existing_comment (str) : Current catalog comment if available
            - new_comment (str)      : AI-generated description
    """
    if not endpoint_name:
        raise ValueError("endpoint_name must be provided for AI query")

    # Step 1: Fetch candidate tables
    query = f"""
        SELECT table_catalog, table_schema, table_name, comment
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog}'
    """
    if schema:
        query += f" AND table_schema = '{schema}'"
    if table:
        query += f" AND table_name = '{table}'"

    tables = spark.sql(query).collect()

    # Step 2: Build descriptions in parallel
    rows = []

    def process_table(t, replace_comment):
        try:
            # Determine if we should actually generate a new description
            should_generate_new_comment = replace_comment or (t["comment"] is None or len(t["comment"]) == 0)
            
            if should_generate_new_comment:
                table_metadata = get_table_metadata(
                    t["table_catalog"], t["table_schema"], t["table_name"], data_limit, max_cell_chars
                )
                ai_desc = get_table_description_ai(table_metadata, endpoint_name, prompt_return_length)
            else: ai_desc = None

            # if not replace_comment:
            #   replace_comment = (t["comment"] is None or len(t["comment"]) == 0)
            
            return Row(
                table_catalog=t["table_catalog"],
                table_schema=t["table_schema"],
                table_name=t["table_name"],
                replace_comment=replace_comment or should_generate_new_comment,
                existing_comment=t["comment"] if t["comment"] else "",
                new_comment=ai_desc
            )
        except Exception as e:
            print(f"FAILED processing {t['table_catalog']}.{t['table_schema']}.{t['table_name']}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_table, t, replace_comment): t for t in tables}
        for f in as_completed(futures):
            result = f.result()
            if result:
                rows.append(result)

    # Step 3: Convert results to Spark DataFrame
    schema_out = StructType([
        StructField("table_catalog", StringType(), True),
        StructField("table_schema", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("replace_comment", BooleanType(), True),
        StructField("existing_comment", StringType(), True),
        StructField("new_comment", StringType(), True)
    ])
    return spark.createDataFrame(rows, schema=schema_out)


# Get table descriptions for catalog, schema, tables
table_descriptions = get_table_descriptions(
        catalog, schema = schema, table = table, data_limit = data_limit, 
        endpoint_name = endpoint_name, max_workers = default_parallelism, 
        replace_comment = always_update, prompt_return_length=prompt_return_length,
        max_cell_chars = max_cell_chars
)
display(table_descriptions)

# COMMAND ----------

# DBTITLE 1,Add and Apply Table Description User Updates
# -----> IMPORTANT <-----
# Pause here and review the table descriptions in the output DF above.  
# Run this cell below to update AI generated table descriptions with your own descriptions.
# -----> IMPORTANT <-----


def apply_table_description_updates(table_descriptions: DataFrame, updates: List[Tuple[str, str, str, str, bool]]) -> DataFrame:
    """
    Apply manual updates to table comments in a Spark DataFrame.
    Args:
        table_descriptions (DataFrame): A Spark DataFrame containing table metadata and comments.
            Must include the following columns:
                - table_catalog (str)
                - table_schema (str)
                - table_name (str)
                - new_comment (str)
                - replace_comment (bool)
        updates (list[tuple]): A list of tuples specifying manual updates.
            Each tuple must have the form:
                (table_catalog, table_schema, table_name, updated_comment, replace_comment_update)
            - updated_comment (str): The new comment to apply to this table.
            - replace_comment_update (bool): Whether to override the replace flag for this table.
    Returns:
        DataFrame: A Spark DataFrame with updated `new_comment` and `replace_comment` values.
                   If `updates` is empty, the original DataFrame is returned unchanged.
    """
    if not updates:
        return table_descriptions

    # Create a DataFrame with the updates
    updates_df = spark.createDataFrame(
        updates,
        ["table_catalog", "table_schema", "table_name", "updated_comment", "replace_comment_update"]
    )

    # Join updates with the original DataFrame and apply updates
    table_descriptions_updated = (
        table_descriptions
        .join(
            updates_df,
            on=["table_catalog", "table_schema", "table_name"],
            how="left"
        )
        .withColumn(
            "new_comment",
            F.when(F.col("replace_comment_update") == F.lit(True), F.col("updated_comment"))
             .otherwise(F.col("new_comment"))
        )
        .withColumn(
            "replace_comment",
            F.when(F.col("replace_comment_update") ==F.lit(True), F.col("replace_comment_update"))
             .otherwise(F.col("replace_comment"))
        )
        .drop("updated_comment", "replace_comment_update")  # cleanup temp cols
    )
    return table_descriptions_updated


# Define your updates as a list of tuples - (catalog, schema, table, updated comment, replace_flag)
replace_comment_flag = False # Change This to True if you want to replace the AI generated comment with your own
updates = [
    (catalog, schema, table, f"This is the new updated comment for {table}.", replace_comment_flag),
    (catalog, schema, table, f"This is the new updated comment for {table}.", replace_comment_flag)
]

# Apply updates
table_descriptions_updated = apply_table_description_updates(table_descriptions, updates).dropDuplicates()
display(table_descriptions_updated)

# COMMAND ----------

# DBTITLE 1,Write AI Generated Table Descriptions to UC Volume
(
    table_descriptions_updated
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .format(output_file_format)
    .save(output_path + f"/{output_file_format}")
)
