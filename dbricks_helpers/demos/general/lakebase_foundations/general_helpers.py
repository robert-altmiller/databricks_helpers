# Databricks notebook source
# DBTITLE 1,Check If a File Exists
def check_file_exists(filepath):
    """Return whether a file or directory exists at *filepath*.
    Args:
        filepath (str | os.PathLike): Absolute or relative path to the
            target file or directory.
    Returns:
        bool: ``True`` if the path exists, otherwise ``False``.
    """
    return os.path.exists(filepath)

# COMMAND ----------

# DBTITLE 1,Read JSON File Using JSON.Loads()
def read_json_file(file_path: str = None, json_str: str = None):
    """
    Reads a JSON file and returns the data as a Python object.
    Parameters:
        file_path (str): Path to the JSON file.
    Returns:
        dict or list: Parsed JSON data.
    """
    try:
        if file_path != None:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        elif json_str != None:
            data = json.loads(json_str)
        return data
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON - {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

# COMMAND ----------

# DBTITLE 1,Serialize Complex Problems into JSON in a Spark Dataframe
def serialize_complex_columns(df):
    """
    Converts all StructType and ArrayType columns in a Spark DataFrame into JSON strings.
    This is useful when writing to sinks (e.g., JDBC) that do not support complex types.
    Parameters:
        df (DataFrame): A PySpark DataFrame that may contain nested or array columns.
    Returns:
        DataFrame: A new DataFrame where all StructType and ArrayType columns are serialized using to_json().
    """
    serialized_df = df
    for field in df.schema.fields:
        # If the field is a StructType or ArrayType, convert it to a JSON string
        if isinstance(field.dataType, (MapType, StructType, ArrayType)):
            serialized_df = serialized_df.withColumn(
                field.name, 
                to_json(col(field.name))
            )
    return serialized_df

# COMMAND ----------

# DBTITLE 1,Convert Create Table Statement  Into Column/Datatype Dict and Primary Key  List
def normalize_type(raw_type: str) -> str:
    """
    Normalizes a raw SQL type string using the TYPE_MAP dictionary.
    Parameters:
        raw_type (str): The raw type string to normalize (e.g., "string", "decimal(10,2)").
    Returns:
        str: The normalized type string (e.g., "TEXT" or "DECIMAL(10,2)").
    Notes:
        - If the type starts with "DECIMAL", it is returned as-is.
        - Otherwise, the base type is extracted and mapped using TYPE_MAP.
    """

    # Type normalization map
    TYPE_MAP = {
        # Scalar types
        "BIGINT": "BIGINT",
        "BINARY": "BYTEA",
        "BOOLEAN": "BOOLEAN",
        "BYTE": "SMALLINT",
        "CHAR": "TEXT",
        "DATE": "DATE",
        "DECIMAL": "DECIMAL",
        "DOUBLE": "DOUBLE PRECISION",
        "FLOAT": "REAL",
        "GEOGRAPHY": "TEXT",
        "GEOMETRY": "TEXT",
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "INTERVAL": "TEXT",
        "LONG": "BIGINT",
        "SHORT": "SMALLINT",
        "STRING": "TEXT",
        "TIMESTAMP": "TIMESTAMP",
        "VARCHAR": "TEXT",
        # Complex and variant types
        "ARRAY": "JSONB",
        "MAP": "JSONB",
        "STRUCT": "JSONB",
        "VARIANT": "JSONB"
    }

    raw_type = raw_type.strip().upper()
    if raw_type.startswith("DECIMAL"):
        return raw_type
    base_type = re.match(r"([A-Z]+)", raw_type)
    if base_type:
        return TYPE_MAP.get(base_type.group(1), raw_type)
    return raw_type


def extract_column_lines_and_pk(sql: str):
    """
    Extracts column definition lines and primary key columns from a CREATE TABLE SQL statement.
    Parameters:
        sql (str): The full CREATE TABLE SQL string.
    Returns:
        tuple[list[str], list[str]]: A tuple where:
            - The first item is a list of column definition lines.
            - The second item is a list of column names used in the PRIMARY KEY constraint.
    Notes:
        - Only parses the first parenthesis block (assumes well-formed SQL).
        - Skips constraint and comment lines when collecting column definitions.
        - Extracts columns listed in `PRIMARY KEY (...)`.
    """
    match = re.search(r'\((.*)\)\s*(COMMENT|\Z)', sql, re.DOTALL | re.IGNORECASE)
    if not match:
        return [], []
    block = match.group(1)

    lines = [line.strip().rstrip(',') for line in block.strip().splitlines()]
    col_lines = []
    primary_keys = []

    for line in lines:
        if "PRIMARY KEY" in line.upper():
            pk_match = re.search(r'\((.*?)\)', line)
            if pk_match:
                primary_keys = [col.strip() for col in pk_match.group(1).split(',')]
        elif not line.upper().startswith("CONSTRAINT") and not line.upper().startswith("COMMENT"):
            col_lines.append(line)

    return col_lines, primary_keys


def convert_create_table_to_dict(sql: str):
    """
    Parses a CREATE TABLE SQL string and returns column type strings and primary key list.
    Parameters:
        sql (str): The full CREATE TABLE SQL statement.
    Returns:
        tuple[dict[str, str], list[str]]: A tuple where:
            - The first item is a dictionary mapping column names to type strings,
              e.g., {"product_id": "TEXT NOT NULL"}
            - The second item is a list of primary key column names.
    """
    col_lines, primary_keys = extract_column_lines_and_pk(sql)
    table_cols = {}
    for col_def in col_lines:
        match = re.match(r"^(\w+)\s+([A-Z0-9_<>,().]+)", col_def, re.IGNORECASE)
        if match:
            col_name, raw_type = match.groups()
            normalized_type = normalize_type(raw_type)
            not_null = "NOT NULL" in col_def.upper()
            final_type = f"{normalized_type} NOT NULL" if not_null else normalized_type
            table_cols[col_name] = final_type
    return table_cols, primary_keys
