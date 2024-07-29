# Databricks notebook source
# DBTITLE 1,Library Imports
import dlt, re, random, string, sys, json
from pyspark.sql.functions import *
from pyspark.sql.types import *


#import utility function to fetch dq rules from the sql server.
sys.path.append('./')
from dlt_dq_generic_utility import get_dq_master_dictionary
from transformation_mappings import *
from dlt_generic_functions import *

# COMMAND ----------

# DBTITLE 1,Input Parameters (Can Have Multiple Different Source Data Formats)
# unity catalog parameters
catalog = "dmp_ddm_dev"
schema = "files_esg"

# data quality parameters
mobile_combustion_pipeline_name = "esg_mobile_combustion"

# example: (bronze_table_name, source_data_path, source_data_type)
bronze_table_source_data = (
    ("mc_comdata", "/mnt/landingzone_ddm_***********/esg_data/mobile_combustion/mms_fleet/comdata/", "csv"),
    #("mc_comdata_delta", "/mnt/landingzone_ddm_**********/esg_data/mobile_combustion/mms_fleet/comdata3/", "delta"),
    #("mc_comdata_parquet", "/mnt/landingzone_ddm_********/esg_data/mobile_combustion/mms_fleet/comdata2/", "parquet"),
    ("mc_penske", "/mnt/landingzone_ddm_**********/esg_data/mobile_combustion/mms_fleet/penske/", "csv"),
    ("mc_ryder", "/mnt/landingzone_ddm_**********/esg_data/mobile_combustion/mms_fleet/ryder/", "csv"),
    ("mc_ontario", "/mnt/landingzone_ddm_********/esg_data/mobile_combustion/mms_fleet/canada_ontario/", "csv"),
    ("mc_quebec", "/mnt/landingzone_ddm_*******/esg_data/mobile_combustion/mms_fleet/canada_quebec/", "csv"),
    ("mc_jetfuel", "/mnt/landingzone_ddm_******/esg_data/mobile_combustion/mms_fleet/jetfuel/", "csv"),
)

# example: (silver_table_name, partition_columns_list, bronze_tables_list, description, data_quality_rules)
mc_dq_rules = get_dq_master_dictionary(spark, mobile_combustion_pipeline_name, "all")
silver_table_source_data = (
    (
        "mobile_combustion_silver", 
        ["quarantined_flag_drop", "quarantined_flag_warn"], 
        ["mc_comdata_bronze", "mc_penske_bronze", "mc_ryder_bronze", "mc_ontario_bronze", "mc_quebec_bronze", "mc_jetfuel_bronze"], 
        "The materialized view (table) will include all the records, each with a quarantine flags.", 
        mc_dq_rules
    ),
)

# example: (gold_table_name, partition_columns_list, silver_tables_list, description, business_quality_rules, aggregate_type)
gold_table_source_data = (
    (
        "mc_consumption_monthly_gold", 
        ["quarantined_flag_drop", "quarantined_flag_warn"], 
        ["mobile_combustion_silver"], 
        "Shows total monthly fuel consumption and cost for different vehicle types.", 
        {'invalid_total_fuel_cost': 'total_fuel_cost > 0'},
        "monthly"
    ),
    (
        "mc_consumption_yearly_gold", 
        ["quarantined_flag_drop", "quarantined_flag_warn"], 
        ["mobile_combustion_silver"], 
        "Shows total yearly fuel consumption and cost for different vehicle types.", 
        {'invalid_total_fuel_cost': 'total_fuel_cost > 0'},
        "yearly"
    ),
)

# COMMAND ----------

# DBTITLE 1,Create Different Streaming Sources for DLT Bronze Tables
def create_stream(method = None, data_source_folder = None, infer_column_types = None, header_exists = None):
    """create a readstream() object based on the source datatype"""
    if method == "delta": # create a delta readStream source
        return (
            spark.readStream.format(method)
            .option("cloudFiles.format", method)
            .option("cloudFiles.inferColumnTypes", infer_column_types)
            .load(data_source_folder)
            .withColumn("load_timestamp", current_timestamp())
        )
    elif method == "parquet": # create a parquet readStream source
        # create a static DataFrame to infer the schema
        df = spark.read.parquet(data_source_folder) # must have defined schema for parquet streaming source
        return (
            spark.readStream.format(method)
            .schema(df.schema)
            .load(data_source_folder)
            .withColumn("load_timestamp", current_timestamp())
        )
    elif method == "csv": # create a csv readStream source
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", method)
            .option("cloudFiles.inferColumnTypes", infer_column_types)
            .option("header", header_exists)
            .load(data_source_folder)
            .select("*", "_metadata.file_path")
            .withColumn("load_timestamp", current_timestamp())
            )


# COMMAND ----------

# DBTITLE 1,Create Bronze Streaming Tables Generic Function
def create_bronze_dlt_streaming(
    table_name = None,
    data_source_folder = None,
    cloud_files_format = None,
    table_description = None,
    table_properties = None,
    header_exists = None,
    infer_column_types = None
):
    """create a bronze streaming source table for dlt pipelines"""

    @dlt.table(
        name = "{}_bronze".format(table_name),
        comment = table_description,
        table_properties = table_properties,
    )
    def create_streaming_bronze_table():
        # read raw data using cloud files (autoloader) and ingest into bronze (streaming) table.
        stream_df = create_stream(cloud_files_format, data_source_folder, infer_column_types, header_exists)

        # replace invalid characters with underscores or other valid characters
        def clean_column_name(name):
            name = name.strip()
            # replace invalid characters with underscores
            cleaned_name = re.sub(r"[^A-Za-z0-9_]", "_", name)
            # replace double underscores with a single underscore
            cleaned_name = re.sub(r"_{2,}", "_", cleaned_name)
            # remove leading and trailing spaces and underscores
            cleaned_name = cleaned_name.strip('_')
            return cleaned_name

        # find duplicate columns
        def find_duplicate_columns(df):
            column_counts = {col_name: df.columns.count(col_name) for col_name in df.columns}
            duplicate_columns = [col_name for col_name, count in column_counts.items() if count > 1]
            return duplicate_columns

        # clean and sanitize column names
        for column_name in list(set(stream_df.columns)):
            new_column_name = clean_column_name(column_name)
            if new_column_name != "" and new_column_name != None:
                stream_df = stream_df.withColumnRenamed(column_name, new_column_name)
        
        # clean up duplicate columns
        duplicate_cols = find_duplicate_columns(stream_df)
        if len(duplicate_cols) > 0: # then duplicate columns exist
            for dupcol in duplicate_cols:
                for column_name in stream_df.columns:
                    if column_name == dupcol:      
                        stream_df = stream_df.withColumn(f"{column_name}_", lit(column_name))
                        stream_df = stream_df.drop(column_name)
                        stream_df = stream_df.withColumnRenamed(f"{column_name}_", column_name)
                        break

        return stream_df

# COMMAND ----------

# DBTITLE 1,Create Silver Streaming Tables Generic Function
def create_dlt_streaming(
    new_table_name = None,
    partition_cols = None,
    input_tables = None,
    table_description = None,
    table_properties = None,
    data_quality_rules = None,
    sql_template = None
):
    """create a silver/gold streaming source table for dlt pipelines"""

    @dlt.table(
        name = new_table_name,
        comment = table_description,
        table_properties = table_properties,
        partition_cols = partition_cols
    )
    # data quality rules from Azure SQL DB
    @dlt.expect_all(data_quality_rules)

    def create_streaming_silver_table():
    
        for input_table_name in input_tables:

            read_df = dlt.readStream(input_table_name)
            read_df.createOrReplaceTempView(input_table_name)
        
        # define the SQL query to transform the bronze data into silver
        new_table = spark.sql(sql_template)
        return new_table

# COMMAND ----------

# DBTITLE 1,Get SQL Template and Column Mappings for SQL Template Injection
def get_sql_template_col_mappings(source_data, catalog, schema, aggtype = None):
    """get sql template and column mappings"""

    sql_template_final = ""
    tblrefs = []
    for table_name in source_data:

        print(f"processing table: {table_name}")
    
        tblref = random.choice(string.ascii_lowercase)
        while tblref in tblrefs:
            tblref = random.choice(string.ascii_lowercase)

        func_name = f"get_col_mapping_{table_name}"
        print(f"column mapping function name: {func_name}()")
        col_mapping, sql_template = get_col_mapping(func_name, table_name, tblref, catalog, schema, aggtype)

        # replace placeholders in the SQL statement with corresponding values from `col_mapping`
        sql_template = insert_col_mappings_into_sql_template(col_mapping, sql_template)
        sql_template_final += sql_template + " UNION ALL "

        tblrefs.append(tblref)

    # remove the trailing " UNION ALL " after the loop
    if sql_template_final.endswith(" UNION ALL "):
        sql_template_final = sql_template_final[:-len(" UNION ALL ")]
    
    return sql_template_final

# COMMAND ----------

# DBTITLE 1,Get Data Quality Rules From Azure SQL DB
# retrieve DQ (Data Quality) dictionaries for warning and drop conditions based on the pipeline name.
# dq_master_dict_all_for_mobile_combustion = get_dq_master_dictionary(spark, mobile_combustion_pipeline_name, "all")
# print(dq_master_dict_all_for_mobile_combustion)

# COMMAND ----------

# DBTITLE 1,Create Bronze Streaming Tables
# call the function with the required arguments.
for table_name, data_source_folder, cloud_files_format in bronze_table_source_data:

    # create a bronze dlt streaming table
    create_bronze_dlt_streaming(
        table_name = table_name,
        data_source_folder = data_source_folder,
        cloud_files_format = cloud_files_format,
        table_description = "New data incrementally ingested from cloud object storage landing zone",
        table_properties = {"quality": "bronze"},
        header_exists = "true",
        infer_column_types = "false"
    )

# COMMAND ----------

# DBTITLE 1,Create Silver Streaming Tables
# call the function with the required arguments.
for silver_table_name, partition_cols, bronze_tables, desc, dq_rules in silver_table_source_data:
    
    # create sql template for silver table creation
    sql_template_final = get_sql_template_col_mappings(bronze_tables, catalog, schema)

    # silver dlt streaming
    create_dlt_streaming(
        new_table_name = silver_table_name,
        partition_cols = partition_cols,
        input_tables = bronze_tables,
        table_description = desc,
        table_properties = {"quality": "silver"},
        data_quality_rules = dq_rules,
        sql_template = sql_template_final
    )

# COMMAND ----------

# DBTITLE 1,Create Gold Streaming Tables
# call the function with the required arguments.
for gold_table_name, partition_cols, silver_tables, desc, dq_rules, aggtype in gold_table_source_data:
    
    # create sql template for gold table creation
    sql_template_final = get_sql_template_col_mappings(silver_tables, catalog, schema, aggtype)
    
    # gold dlt streaming
    create_dlt_streaming(
        new_table_name = gold_table_name,
        partition_cols = partition_cols,
        input_tables = silver_tables,
        table_description = desc,
        table_properties = {"quality": "gold"},
        data_quality_rules = dq_rules,
        sql_template = sql_template_final
    )

# COMMAND ----------

# DBTITLE 1,Create Dim Tables From Silver
table_name = "mobile_combustion_silver"
table_ref = "a"
select_columns = ["msdyn_name", "msdyn_dataqualitytype", "msdyn_organizationalunitid", "msdyn_fueltypeid", "msdyn_vehicletype", "msdyn_fueladdress", "msdyn_fuelzip"] # total fact 
group_by_columns = select_columns

# parameters: table_name, table_ref, select_columns, group_by_columns, where_clauses
# group_by_columns example: ["col1", "col2", "col3", "COUNT(*)"]
# where_clauses example: ["col1 > 0 AND", "col2 != Null OR", "col3 < 100"]
sql_template = generate_dynamic_sql(table_name, table_ref, select_columns, group_by_columns, None)
col_mapping = create_dynamic_sql_dict(table_name, table_ref, select_columns)

sql_template = insert_col_mappings_into_sql_template(col_mapping, sql_template)

# silver dlt streaming
create_dlt_streaming(
    new_table_name = "mobile_combustion_dim_silver",
    partition_cols = [],
    input_tables = [table_name],
    table_description = "mobile_combustion_silver dimension table",
    data_quality_rules = {},
    sql_template = sql_template
)