def calculate_total_folder_size_and_count(spark, directory_path: str, extension: str = ".mcap"):
    """
    Calculates the total size and counts the number of files with a specific extension within the specified directory.

    Inputs:
    - spark: Spark session object.
    - directory_path (str): The directory path where the files are located.
    - extension (str): The file extension to filter by (default is '.mcap').

    Outputs:
    - Returns a DataFrame containing the count and total size (in bytes, MB, GB, and TB) of files matching the extension.

    Example Usage:
    result_df = calculate_total_folder_size_and_count(spark, "/mnt/brt-dune/dune_dev/sanger/logs/data/drive_id=18/")
    display(result_df)
    """
    from pyspark.sql.functions import col, sum, count
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    # Define conversion factors
    MB_FACTOR = float(1024 ** 2)
    GB_FACTOR = float(1024 ** 3)
    TB_FACTOR = float(1024 ** 4)
    # List all files in the specified directory
    files = dbutils.fs.ls(directory_path)
    # Create a DataFrame with file paths and sizes, filtering only files with the specified extension
    files_df = spark.createDataFrame(files).select('path', 'size').filter(col("path").endswith(extension))
    # Aggregate the sizes of these files to compute the total size and convert units
    total_size_count_df = files_df.agg(
        count('path').alias(f"total_{extension.strip('.')}s"),
        sum('size').alias('total_size_bytes'),
        (sum('size') / MB_FACTOR).alias('total_size_mb'),
        (sum('size') / GB_FACTOR).alias('total_size_gb'),
        (sum('size') / TB_FACTOR).alias('total_size_tb')
    )
    # Return the DataFrame containing the aggregated total size and file count
    return total_size_count_df


def is_running_in_databricks():
    """Check if code is running in Databricks or locally"""
    import os
    # Databricks typically sets these environment variables
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        print("code is running in databricks....\n")
        return True
    else:
        #print("code is running locally....\n")
        return False


def get_external_ip():
    """
    Determines the external IP address of the machine by creating a UDP socket
    and connecting to an external server. This method ensures the correct IP 
    is captured by using the external network interface.

    Inputs:
    - None

    Outputs:
    - Returns the external IP address as a string. If unable to determine the 
      IP address, returns '127.0.0.1' as a fallback.

    Example Usage:
    ip_address = get_external_ip()
    print(f"External IP: {ip_address}")
    """
    import socket
    # Create a UDP socket using IPv4 addressing (AF_INET)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Attempt to connect to an external server (Google's DNS) at port 80
        s.connect(("8.8.8.8", 80))

        # Retrieve the local IP address from the socket's connection details
        ip_address = s.getsockname()[0]
    except Exception:
        # If an error occurs (e.g., no network), use '127.0.0.1' as a fallback
        ip_address = "127.0.0.1"
    finally:
        # Close the socket to free up resources
        s.close()

    # Return the determined IP address
    return ip_address


def schema_to_string(schema):
    """
    Recursively converts a Spark StructType schema into the string format required by @pandas_udf.
    
    Inputs:
    - schema (StructType): A Spark StructType schema representing the structure of the DataFrame.

    Outputs:
    - Returns a string representation of the schema, formatted to be compatible with the @pandas_udf annotation.

    Example Usage:
    schema_str = schema_to_string(my_schema)
    print(schema_str)
    
    The function handles various Spark data types such as StringType, BooleanType, IntegerType, StructType (nested structures), 
    and ArrayType, recursively converting each field to the appropriate string format.
    """
    from pyspark.sql.types import StructType, StructField, ArrayType, StringType, BooleanType, IntegerType
    def field_to_string(field):
        # Base case: Convert simple data types to their string equivalents
        if isinstance(field.dataType, StringType):
            return "string"
        elif isinstance(field.dataType, BooleanType):
            return "boolean"
        elif isinstance(field.dataType, IntegerType):
            return "int"
        # Recursive case: Convert StructType to a nested string format for nested structures
        elif isinstance(field.dataType, StructType):
            return f"struct<{','.join([f'{subfield.name}:{field_to_string(subfield)}' for subfield in field.dataType.fields])}>"
        # Recursive case: Convert ArrayType to a format that represents an array of another type
        elif isinstance(field.dataType, ArrayType):
            element_type = field_to_string(StructField("", field.dataType.elementType))
            return f"array<{element_type}>"
        # Raise error for unsupported data types
        else:
            raise TypeError(f"Unsupported data type: {field.dataType}")
    
    # Convert the top-level schema to a string format compatible with @pandas_udf
    return f"struct<{','.join([f'{field.name}:{field_to_string(field)}' for field in schema.fields])}>"


def bytes_to_mb(bytes_size):
    """
    Convert bytes to megabytes (MB).
    
    Inputs:
    - bytes_size (int): Size in bytes to be converted to MB.

    Outputs:
    - float: Size in megabytes, rounded to two decimal places.

    Example Usage:
    mb_size = bytes_to_mb(1048576)
    print(mb_size)  # Output: 1.0 for 1 MB
    """
    return round(bytes_size / (1024 * 1024), 2)


def get_total_executors(spark):
    """
    Retrieves the total number of executors currently running in a Spark cluster, excluding the driver.

    Inputs:
    - spark: Spark session object.

    Outputs:
    - Returns an integer representing the total number of worker executors, excluding the driver.

    Example Usage:
    total_executors = get_total_executors(spark)
    print(f"Total number of executors: {total_executors}")
    """
    
    # Get the SparkContext from the Spark session
    sc = spark.sparkContext
    # Get the executor memory status from the SparkContext.
    # This is a Scala Map where the keys are the executor identifiers (e.g., "IP:port")
    # and the values are memory-related information for each executor.
    executor_memory_status = sc._jsc.sc().getExecutorMemoryStatus()
    # Initialize an iterator for the keys in the Scala map (the executor IDs)
    keys = executor_memory_status.keySet().iterator()
    # Extract all executor IDs into a list
    executors = []
    while keys.hasNext():
        key = keys.next()  # Executor ID (e.g., "IP:port")
        executors.append(key)
    # Calculate the total number of executors by excluding the driver
    # The driver is also listed as an executor, so we subtract 1 to exclude it
    total_executors = len(executors) - 1
    # Return the total number of worker executors
    if total_executors == 0: return 1
    else: return total_executors



def get_delta_table_details(spark, table_name):
    """
    Fetch details about a Delta table, specifically its size in MB and the number of Parquet files.
    
    Inputs:
    - spark: SparkSession object.
    - table_name (str): Name of the Delta table to analyze.

    Outputs:
    - tuple (float, int): Total size of the table in MB and total number of Parquet files in the table.

    Example Usage:
    table_size, num_files = get_delta_table_details(spark, "my_delta_table")
    print(f"Table Size: {table_size} MB, Number of Parquet Files: {num_files}")
    """
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forName(spark, table_name)
    table_details = delta_table.detail().select("sizeInBytes", "numFiles").collect()[0]
    return bytes_to_mb(table_details["sizeInBytes"]), table_details["numFiles"]
