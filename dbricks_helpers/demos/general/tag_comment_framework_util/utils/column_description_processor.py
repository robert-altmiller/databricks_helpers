"""
Column Description Processor
"""

from typing import Dict, List
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType


class ColumnDescriptionGenerator:
    """Generate AI column descriptions and save to storage."""
    
    def __init__(self, spark, catalog, output_path,
                 endpoint_name="databricks-meta-llama-3-3-70b-instruct",
                 schema=None, table=None,
                 data_limit=5, max_cell_chars=1000, always_update=True,
                 prompt_return_length=40, output_file_format="json"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.output_path = output_path + "/bulk_comments/columns/"
        self.endpoint_name = endpoint_name
        self.data_limit = data_limit
        self.max_cell_chars = max_cell_chars
        self.always_update = always_update
        self.prompt_return_length = prompt_return_length
        self.output_file_format = output_file_format
    
    def get_column_info(self) -> List[Dict]:
        """Get column metadata and sample data (matches original logic exactly)."""
        full_table_name = f"{self.catalog}.{self.schema}.{self.table}"
        
        # Get column metadata
        columns_df = self.spark.sql(f"""
            SELECT column_name, data_type, comment
            FROM system.information_schema.columns
            WHERE table_catalog = '{self.catalog}'
            AND table_schema = '{self.schema}'
            AND table_name = '{self.table}'
            ORDER BY ordinal_position
        """)
        
        columns = columns_df.collect()
        
        # Get sample data (matching original logic with recursive dict and truncation)
        sample_rows = []
        try:
            data_query = f"SELECT * FROM {full_table_name} LIMIT {self.data_limit}"
            rows_raw = [row.asDict(recursive=True) for row in self.spark.sql(data_query).collect()]
            
            for row_dict in rows_raw:
                clean_row = {}
                for col, val in row_dict.items():
                    if val is None:
                        clean_row[col] = None
                    else:
                        val_str = str(val)
                        # Truncate overly long individual column values only
                        if len(val_str) > self.max_cell_chars:
                            clean_row[col] = val_str[:self.max_cell_chars] + " ...[truncated]"
                        else:
                            clean_row[col] = val_str
                sample_rows.append(clean_row)
        except:
            sample_rows = []
        
        # Build column info
        column_info = []
        for col in columns:
            col_name = col["column_name"]
            column_info.append({
                "column_name": col_name,
                "data_type": col["data_type"],
                "existing_comment": col["comment"] if col["comment"] else "",
                "sample_rows": sample_rows  # Store full rows for context
            })
        
        return column_info
    
    def get_column_description_ai(self, col_info: Dict) -> str:
        """Generate AI description for a column using EXACT ORIGINAL template."""
        # Extract sample values for this specific column from the sample rows
        sample_rows = col_info['sample_rows'][:3]  # Use only first 3 rows for brevity
        sample_text = str(sample_rows)
        
        prompt = (
            f'This description will be stored as a Unity Catalog table column comment. '
            f'Write a detailed, single-sentence description of approximately {self.prompt_return_length} words '
            f'for the column "{col_info["column_name"]}" from the table "{self.table}" in schema "{self.schema}" '
            f'within catalog "{self.catalog}". '
            f'This description should clearly explain what kind of information the column contains and its purpose. '
            f'The column data type is "{col_info["data_type"]}". '
            f'Use the following sample rows for context: {sample_text}. '
            f'Keep the description professional and concise, suitable for a data dictionary. '
            f'Do not mention schema or catalog names in the output.'
        )

        result = self.spark.sql(f"""
            SELECT ai_query(
                '{self.endpoint_name}',
                '{prompt.replace("'", "''")}'
            ) as description
        """).collect()[0]['description']
        
        return result.strip()
    
    def execute(self) -> DataFrame:
        """Generate descriptions for all columns in the table."""
        columns = self.get_column_info()
        
        rows = []
        for col_info in columns:
            # Check if we need to generate description
            should_generate = self.always_update or not col_info["existing_comment"]
            
            if should_generate:
                new_comment = self.get_column_description_ai(col_info)
            else:
                new_comment = None
            
            rows.append(Row(
                table_catalog=self.catalog,
                table_schema=self.schema,
                table_name=self.table,
                column_name=col_info["column_name"],
                data_type=col_info["data_type"],
                replace_comment=should_generate,
                existing_comment=col_info["existing_comment"],
                new_comment=new_comment
            ))
        
        schema = StructType([
            StructField("table_catalog", StringType(), True),
            StructField("table_schema", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("replace_comment", BooleanType(), True),
            StructField("existing_comment", StringType(), True),
            StructField("new_comment", StringType(), True)
        ])
        
        return self.spark.createDataFrame(rows, schema=schema)
    
    def save_comments(self, commented_columns: DataFrame) -> None:
        """Save column comments to storage."""
        output_location = f"{self.output_path}/{self.output_file_format}/{self.table}"
        
        (
            commented_columns
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .format(self.output_file_format)
            .save(output_location)
        )


class ColumnDescriptionImporter:
    """Import and apply column descriptions to Unity Catalog."""
    
    def __init__(self, spark, input_path, output_file_format="json"):
        self.spark = spark
        self.input_path = input_path
        self.output_file_format = output_file_format
    
    def read_comments(self) -> DataFrame:
        """Read comments from storage."""
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
    
    def prepare_comments(self, column_comments: DataFrame) -> DataFrame:
        """Group comments by table."""
        return (column_comments
            .withColumn("full_table_name",
                F.concat(F.col("table_catalog"), F.lit('.'),
                        F.col("table_schema"), F.lit('.'),
                        F.col("table_name")))
            .withColumn("cleaned_comment", F.regexp_replace("new_comment", "'", "")))
    
    def apply_comments(self, prepared_df: DataFrame) -> int:
        """Apply comments to Unity Catalog columns. Returns count of successful updates."""
        # Group by table
        tables = prepared_df.select("full_table_name").distinct().collect()
        successful_count = 0
        
        for table_row in tables:
            table_name = table_row["full_table_name"]
            table_columns = prepared_df.filter(F.col("full_table_name") == table_name).collect()
            
            for col in table_columns:
                col_name = col["column_name"]
                comment = col["cleaned_comment"]
                
                try:
                    self.spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN `{col_name}` COMMENT '{comment}'")
                    successful_count += 1
                except Exception as e:
                    print(f"  ✗ Failed: {table_name}.{col_name} - {str(e)[:100]}")
        
        return successful_count
    
    def execute(self) -> Dict:
        """Execute the import workflow. Returns dictionary with successful_columns count."""
        comments = self.read_comments()
        
        if comments.count() == 0:
            return {'successful_columns': 0}
        
        prepared = self.prepare_comments(comments)
        successful_count = self.apply_comments(prepared)
        
        return {'successful_columns': successful_count}
