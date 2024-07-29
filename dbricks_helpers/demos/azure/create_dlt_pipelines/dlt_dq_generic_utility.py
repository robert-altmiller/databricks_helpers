SERVER = ""
DATABASE = ""
DB_USER = ""
DB_PASS = ""

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Function to create JDBC URL
def create_jdbc_url(server, database):
    return f"jdbc:sqlserver://{server};databaseName={database}"

# Function to build SQL query
def build_query(table_name, pipeline_name):
    return f"(SELECT pipeline_name,col_name,rule_name,rule_type,rule_val FROM {table_name} WHERE pipeline_name='{pipeline_name}' AND is_active='true') AS temp_table"

# Function to read data from JDBC table
def read_jdbc_table(spark, jdbc_url, query, db_user, db_pass):
    options = {
        "url": jdbc_url,
        "dbtable": query,
        "user": db_user,
        "password": db_pass
    }
    return spark.read.jdbc(url=jdbc_url, table=query, properties=options)

#Get DQ details from tehe metadata table
def get_dq_master_data(spark, pipeline_name):
     # Set up JDBC URL
    jdbc_url = create_jdbc_url(SERVER, DATABASE)

    # Build SQL query
    query = build_query("DLT_DQ_METADATA", pipeline_name)

    try:
        # Read data from JDBC table
        dq_df = read_jdbc_table(spark, jdbc_url, query, DB_USER, DB_PASS)
        
        # Display the DataFrame
        return dq_df
    except Exception as e:
        # Handle exceptions
        print("An error occurred:", str(e))


# Convert PySpark DataFrame to Python dictionary
def create_rule_dictionary(df):
    rule_dict = {}
    for row in df.collect():
        rule_name = row["rule_name"]
        rule_val = row["rule_val"]
        col_name = row["col_name"]
        rule_dict[rule_name] = f"{col_name} {rule_val}"
    return rule_dict

# prepare DQ python dictionary for the mentioned pipelines
def get_dq_master_dictionary(spark, pipeline_name, dq_rule_type):
    # Get dq master data from the metadata table 
    dq_master_df_raw = get_dq_master_data(spark, pipeline_name)

    # Get DataFrames directly by filtering rule types
    if(dq_rule_type != 'all'):
        dq_master_df_raw = dq_master_df_raw.filter(col("rule_type").like(dq_rule_type))

    # Create dictionaries using the function
    dq_master_dict = create_rule_dictionary(dq_master_df_raw)
    
    return dq_master_dict