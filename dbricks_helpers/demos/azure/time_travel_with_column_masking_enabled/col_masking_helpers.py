# Databricks notebook source
# DBTITLE 1,Create Column Masking Policy
def create_col_mask_policy(catalog: str, schema: str, table_name: str, mask_name: str, mask_rule: str, user_email: str = None, group_name: str = None):
    """
    Creates or replaces a Unity Catalog column-level masking policy in Databricks.
    The masking policy reveals the original value only to a specific user or group.
    Otherwise, it masks the value using a partial SSN format (e.g., 'XXX-XX-1234').
    Parameters:
        catalog (str): The Unity Catalog catalog name.
        schema (str): The schema (database) within the catalog.
        mask_name (str): The name of the masking policy to create.
        mask_rule (str): The masking rule to apply.  Val is masking value.
        user_email (str, optional): A specific user email allowed to see full values.
        group_name (str, optional): A group allowed to see full values (if user_email is not provided).
    Returns:
        None. Executes the SQL statement to create the masking policy in Spark.
    """
    table_fqn = f"{catalog}.{schema}.{table}"
    mask_name_fqn = f"{catalog}.{schema}.{mask_name}"
    if user_email != None:
        case_eval =f"current_user() IN ('{user_email}')"
    else: case_eval = f"is_account_group_member('{group_name}')"
    SQL = f"""
        CREATE OR REPLACE FUNCTION {mask_name_fqn}(val STRING) RETURNS STRING
        RETURN
            CASE
                WHEN {case_eval} THEN val
                ELSE {mask_rule}
            END;
        """
    spark.sql(SQL)
    print(f"created column masking policy '{mask_name_fqn}' on catalog.schema '{catalog}.{schema}' when '{case_eval}'")
    return table_fqn, mask_name_fqn

# COMMAND ----------

# DBTITLE 1,Apply Column Masking Policy
def apply_col_mask_policy(table_fqn: str, mask_name_fqn: str, col_name: str, apply_col_mask: bool = False):
    """
    Applies a column masking policy to a specific column in a Unity Catalog table.
    Args:
        table_fqn (str): Fully qualified name of the target table (e.g., 'catalog.schema.table').
        mask_name_fqn (str): Fully qualified name of the masking function (e.g., 'catalog.schema.mask_function').
        col_name (str): Name of the column to which the mask should be applied.
        apply_col_mask (bool): If True, the mask will be applied. Defaults to False to avoid accidental changes.
    Returns:
        None
    """
    if apply_col_mask:
        # Run the SQL command to apply the masking policy to the column
        spark.sql(f"ALTER TABLE {table_fqn} ALTER COLUMN {col_name} SET MASK {mask_name_fqn}")
        print(f"✅ applied column masking policy {mask_name_fqn} to table {table_fqn}\n")

# COMMAND ----------

# DBTITLE 1,Remove Column Masking Policy
def remove_col_mask_policy(masked_cols_dict: dict):
    """
    Removes column-level masking policies from one or more columns across one or more Delta tables.
    Parameters:
        masked_cols_dict (dict): A dictionary where:
            - Keys are fully qualified table names (e.g., 'main.pii_demo.synthetic_pii')
            - Values are lists of column names to remove masking from
    Example:
        remove_col_mask_policy({
            "main.pii_demo.synthetic_pii": ["ssn", "email"],
            "main.hr.employees": ["phone"]
        })
    """
    # Iterate through each table and its corresponding masked columns
    for table_fqn, cols in masked_cols_dict.items():
        for col in cols:
            # Execute SQL to drop the masking policy from the specified column
            spark.sql(f"ALTER TABLE {table_fqn} ALTER COLUMN {col} DROP MASK;")
            print(f"✅ Removed column masking policy from table '{table_fqn}' and column '{col}'")

# COMMAND ----------

# DBTITLE 1,Get Existing Table Column Masking Policies
def get_column_masking_policies(table_fqn: str) -> dict:
    """
    Returns a dictionary of columns with their associated masking policies for the given Unity Catalog table.
    Args:
        table_fqn (str): Fully qualified table name (e.g., 'catalog.schema.table').
    Returns:
        dict: A dictionary where keys are column names and values are the applied masking policy function names.
    """
    # Run DESCRIBE TABLE EXTENDED to get table metadata, including any column masks
    result = spark.sql(f"DESCRIBE TABLE EXTENDED {table_fqn}").collect()

    in_column_masks = False  # Flag to track when we're in the "# Column Masks" section
    masking_policies = {}    # Final result dictionary

    for row in result:
        # Detect the start of the column masks section
        if "# Column Masks" in row.col_name:
            in_column_masks = True
            continue
        if in_column_masks:
            # If we hit another metadata section, stop collecting
            if row.col_name.startswith("#"):
                break
            # Collect the column name and its masking function (in the data_type field)
            if row.col_name and row.data_type:
                masking_policies[row.col_name.strip()] = row.data_type.strip()
    return masking_policies


# COMMAND ----------

# DBTITLE 1,Reconstruct Existing Table Column Masking SQL Functions

def reconstruct_masking_function_sql(function_fqn: str, format_with_sqlfluff: bool = True) -> str:
    """
    Reconstructs the CREATE FUNCTION SQL from the output of DESCRIBE FUNCTION EXTENDED in Spark,
    with optional formatting using sqlfluff.
    Args:
        function_fqn (str): Fully qualified function name (e.g. catalog.schema.function_name)
        format_with_sqlfluff (bool): Whether to auto-format the SQL using sqlfluff.
    Returns:
        str: Well-formatted CREATE OR REPLACE FUNCTION SQL statement.
    """

    # Fetch raw function description from Spark
    desc_df = spark.sql(f"DESCRIBE FUNCTION EXTENDED {function_fqn}")
    rows = [row["function_desc"] for row in desc_df.collect()]

    func_name   = ""
    input_sig   = ""
    return_type = ""
    body_lines  = []
    capture_body = False

    for line in rows:
        if line.startswith("Function:"):
            func_name = line.split("Function:")[1].strip()
        elif line.startswith("Input:"):
            input_sig = line.split("Input:")[1].strip()
        elif line.startswith("Returns:"):
            return_type = line.split("Returns:")[1].strip()
        elif line.startswith("Body:"):
            capture_body = True
            body_line = line.split("Body:")[1].strip()
            if body_line:
                body_lines.append(body_line)
        elif capture_body:
            if line.strip() == "" or ":" in line:
                break
            body_lines.append(line.strip())

    raw_sql = (
        f"CREATE OR REPLACE FUNCTION {func_name}({input_sig})\n"
        f"RETURNS {return_type}\n"
        f"RETURN\n"
        f"  {' '.join(body_lines)}"
    )

    if format_with_sqlfluff:
        lint_result = sqlfluff.lint(raw_sql, dialect="databricks")
        fix_result = sqlfluff.fix(raw_sql, dialect="databricks")
        return fix_result.strip()
    
    return raw_sql.strip()