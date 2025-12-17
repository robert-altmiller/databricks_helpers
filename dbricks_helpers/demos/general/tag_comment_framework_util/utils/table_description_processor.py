"""
Table Description Processor
"""

from typing import Dict, Optional
from functools import reduce
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType


class TableDescriptionGenerator:
    """Generate AI table descriptions and save to storage."""
    
    def __init__(self, spark, catalog, output_path,
                 endpoint_name="databricks-meta-llama-3-3-70b-instruct",
                 schema=None, table=None,
                 data_limit=2, max_cell_chars=1000, always_update=True,
                 prompt_return_length=200, output_file_format="json"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.output_path = output_path + "/bulk_comments/tables"
        self.endpoint_name = endpoint_name
        self.data_limit = data_limit
        self.max_cell_chars = max_cell_chars
        self.always_update = always_update
        self.prompt_return_length = prompt_return_length
        self.output_file_format = output_file_format
        
    def get_table_metadata(self, catalog: str, schema: str, table: str) -> Dict:
        """Get table schema and sample data (EXACT ORIGINAL logic)."""
        full_table_name = f"{catalog}.{schema}.{table}"
        
        # Load table as DataFrame
        df = self.spark.table(full_table_name)
        
        # Build schema string: "col1 type1, col2 type2, ..."
        schema_str = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields])
        
        # Get sample data with truncation (matching original exactly)
        samples = []
        
        # Collect up to data_limit rows from the table
        for row in df.limit(self.data_limit).collect():
            rdict = {}
            
            # Iterate through each column in the row
            for col, val in row.asDict(recursive=True).items():
                if val is None:
                    # Preserve null values as-is
                    rdict[col] = None
                else:
                    val_str = str(val)
                    
                    # Truncate only if individual value exceeds the configured limit
                    if len(val_str) > self.max_cell_chars:
                        rdict[col] = val_str[:self.max_cell_chars] + " ...[truncated]"
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
            "samples": samples
        }
    
    def get_table_description_ai(self, table_metadata: Dict) -> str:
        """Generate AI description using metadata with business-focused template (EXACT ORIGINAL)."""
        sample_preview = str(table_metadata["samples"])
        
        prompt = (
            f'Provide the business context / definition, related business processes, and how to use / business enablement for the Table Metadata below using the format below:\n\n'
            
            f'**BUSINESS CONTEXT / DEFINITION**:\n'
            f'[Example Format: - This data provides information about customer choices for in-season products, including details about product distribution, allocation, and lifecycle. It helps businesses understand how products are being distributed, allocated, and managed throughout their lifecycle, enabling informed decisions about product offerings, inventory management, and customer satisfaction.]\n'
            f'**RELATED BUSINESS PROCESSES**:\n'
            f'[Example Format: - Product Distribution and Allocation: This data supports the process of distributing products to various channels and allocating them to specific customer groups.]\n'
            f'**HOW TO USE / BUSINESS ENABLEMENT**:\n'
            f'[Example Format: - Use this data to analyze product distribution patterns and identify areas for improvement in allocation and inventory management..]\n'
            
            f'\nStrict Requirements:\n\n'
            f'- Use concise business language suitable for non-technical business users and avoid technical jargon in all the descriptions above.\n'
            f'- Do not use catalog names, schema names, table names, or data types in any of the descriptions above.\n'
            f'- The entire output should be <= {self.prompt_return_length} words in all the descriptions above.\n'
            
            f'\nTable Metadata:\n\n'
            f'- The table "{table_metadata["table"]}" is in schema "{table_metadata["schema"]}" within catalog "{table_metadata["catalog"]}".\n'
            f'- The table columns are {table_metadata["schema_str"]}.\n'
            f'- The sample data is: {sample_preview}.\n'
        )
        
        result = self.spark.sql(f"""
            SELECT ai_query(
                '{self.endpoint_name}',
                '{prompt.replace("'", "''")}'
            ) as description
        """).collect()[0]['description']
        
        return result.strip()
    
    def execute(self) -> DataFrame:
        """Generate description for the specified table."""
        # Get table info
        query = f"""
            SELECT table_catalog, table_schema, table_name, comment
            FROM system.information_schema.tables
            WHERE table_catalog = '{self.catalog}'
            AND table_schema = '{self.schema}'
            AND table_name = '{self.table}'
        """
        
        table_info = self.spark.sql(query).collect()
        if not table_info:
            raise Exception(f"Table not found: {self.catalog}.{self.schema}.{self.table}")
        
        t = table_info[0]
        
        # Check if we need to generate description
        should_generate = self.always_update or (t["comment"] is None or len(t["comment"]) == 0)
        
        if should_generate:
            metadata = self.get_table_metadata(t["table_catalog"], t["table_schema"], t["table_name"])
            new_comment = self.get_table_description_ai(metadata)
        else:
            new_comment = None
        
        # Create result row
        row = Row(
            table_catalog=t["table_catalog"],
            table_schema=t["table_schema"],
            table_name=t["table_name"],
            replace_comment=should_generate,
            existing_comment=t["comment"] if t["comment"] else "",
            new_comment=new_comment
        )
        
        schema = StructType([
            StructField("table_catalog", StringType(), True),
            StructField("table_schema", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("replace_comment", BooleanType(), True),
            StructField("existing_comment", StringType(), True),
            StructField("new_comment", StringType(), True)
        ])
        
        return self.spark.createDataFrame([row], schema=schema)
    
    def save_descriptions(self, table_descriptions: DataFrame) -> None:
        """Save descriptions to storage."""
        output_location = f"{self.output_path}/{self.output_file_format}/{self.table}"
        
        (
            table_descriptions
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .format(self.output_file_format)
            .save(output_location)
        )


class TableDescriptionImporter:
    """Import and apply table descriptions to Unity Catalog."""
    
    def __init__(self, spark, input_path, output_file_format="json", table_list=None):
        self.spark = spark
        self.input_path = input_path
        self.output_file_format = output_file_format
        self.table_list = table_list  # Optional: list of specific tables to import
    
    def read_descriptions(self) -> DataFrame:
        """Read descriptions from storage."""
        # If specific tables provided, read only those
        if self.table_list:
            dfs = []
            for table_full_name in self.table_list:
                # Extract table name from catalog.schema.table
                table_name = table_full_name.split('.')[-1]
                try:
                    df = (self.spark.read
                        .format(self.output_file_format)
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .load(f"{self.input_path}/{table_name}"))
                    dfs.append(df)
                except:
                    pass  # Skip tables that couldn't be read
            
            if dfs:
                df = reduce(lambda df1, df2: df1.union(df2), dfs)
            else:
                # Return empty DataFrame with correct schema
                schema = StructType([
                    StructField("table_catalog", StringType(), True),
                    StructField("table_schema", StringType(), True),
                    StructField("table_name", StringType(), True),
                    StructField("replace_comment", BooleanType(), True),
                    StructField("existing_comment", StringType(), True),
                    StructField("new_comment", StringType(), True)
                ])
                df = self.spark.createDataFrame([], schema)
        else:
            # Read all tables (original behavior)
            try:
                # Read from all table subfolders
                df = (self.spark.read
                    .format(self.output_file_format)
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(self.input_path + "/*"))
            except:
                # Fallback to direct path
                df = (self.spark.read
                    .format(self.output_file_format)
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(self.input_path))
        
        return df.filter(F.col("replace_comment") == True)
    
    def prepare_descriptions(self, table_descriptions: DataFrame) -> DataFrame:
        """Add helper columns for UC updates."""
        return (table_descriptions
            .withColumn("full_table_name",
                F.concat(F.col("table_catalog"), F.lit('.'),
                         F.col("table_schema"), F.lit('.'),
                         F.col("table_name")))
            .withColumn("cleaned_comment", F.regexp_replace("new_comment", "'", "")))
    
    def apply_descriptions(self, prepared_df: DataFrame) -> int:
        """Apply descriptions to Unity Catalog tables. Returns count of successful updates."""
        tables = prepared_df.collect()
        successful_count = 0
        
        for row in tables:
            full_name = row["full_table_name"]
            comment = row["cleaned_comment"]
            
            try:
                self.spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")
                successful_count += 1
            except Exception as e:
                print(f"  ✗ Failed: {full_name} - {str(e)[:100]}")
        
        return successful_count
    
    def execute(self) -> Dict:
        """Execute the import workflow. Returns dictionary with tables_processed count."""
        descriptions = self.read_descriptions()
        
        if descriptions.count() == 0:
            return {'tables_processed': 0}
        
        prepared = self.prepare_descriptions(descriptions)
        successful_count = self.apply_descriptions(prepared)
        
        return {'tables_processed': successful_count}
