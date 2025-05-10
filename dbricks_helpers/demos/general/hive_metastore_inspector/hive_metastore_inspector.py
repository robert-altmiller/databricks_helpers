# Databricks notebook source
# DBTITLE 1,Library Imports
import json, threading
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

# DBTITLE 1,Hive Metastore Inspector Class
class HiveMetastoreInspector:
    """
    A utility class to extract and inspect metadata for all Hive Metastore tables in a Databricks workspace.

    Attributes:
        spark (SparkSession): Active Spark session used to run SQL queries.
        max_threads (int): Number of threads used for parallel metadata extraction.
        schema (StructType): Explicit schema used to define the structure of the output DataFrame.
    """

    def __init__(self, spark: SparkSession, max_threads: int = 32):
        """
        Initialize the HiveMetastoreInspector.

        Args:
            spark (SparkSession): The Spark session object.
            max_threads (int, optional): Number of threads to use for parallelism. Defaults to 32.
        """
        self.spark = spark
        self.max_threads = max_threads

        self.schema = StructType([
            StructField("table_fqn", StringType(), True),
            StructField("catalog", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("last_access", StringType(), True),
            StructField("created_by", StringType(), True),
            StructField("type", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("location", StringType(), True),
            StructField("provider", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("is_managed_location", StringType(), True),
            StructField("properties", MapType(StringType(), StringType()), True),
        ])

    def _extract_table_metadata(self, db: str, tbl: str) -> Row:
        """
        Internal helper function to extract metadata for a specific table using DESCRIBE TABLE EXTENDED.

        Args:
            db (str): Database/schema name.
            tbl (str): Table name.

        Returns:
            Row: A PySpark Row containing metadata fields matching the defined schema.
        """
        try:
            rows = self.spark.sql(f"DESCRIBE TABLE EXTENDED `{db}`.`{tbl}`").collect()

            metadata = {
                "table_fqn": f"hive_metastore.{db}.{tbl}",
                "catalog": None,
                "schema": db,
                "table": tbl,
                "created_at": None,
                "last_access": None,
                "created_by": None,
                "type": None,
                "comment": None,
                "location": None,
                "provider": None,
                "owner": None,
                "is_managed_location": None,
                "properties": {},
            }

            in_metadata = False

            for row in rows:
                col = (row.col_name or "").strip()
                val = (row.data_type or "").strip()

                if col == "# Detailed Table Information":
                    in_metadata = True
                    continue
                if in_metadata and col.startswith("#"):
                    break

                if in_metadata:
                    field = col.lower().replace(" ", "_")
                    if field == "table_properties":
                        val = val.strip("[]")
                        for item in val.split(","):
                            if "=" in item:
                                k, v = item.split("=", 1)
                                metadata["properties"][k.strip()] = v.strip()
                    elif field in metadata:
                        if field == "catalog":
                            metadata[field] = "hive_metastore"
                        else: metadata[field] = val

            return Row(**metadata)

        except Exception as e:
            return Row(
                table_fqn=f"hive_metastore.{db}.{tbl}",
                catalog="hive_metastore",
                schema=db,
                table=tbl,
                created_at="ERROR",
                last_access="ERROR",
                created_by="ERROR",
                type="ERROR",
                comment="ERROR",
                location="ERROR",
                provider="ERROR",
                owner="ERROR",
                is_managed_location="ERROR",
                properties={"error": str(e)},
            )

    def collect_metadata(self, return_type: str = "dataframe"):
        """
        Collects metadata for all tables in the Hive Metastore.

        Args:
            return_type (str): Determines output format:
                - "dataframe": returns a Spark DataFrame (default)
                - "json": returns a list of dictionaries

        Returns:
            Union[DataFrame, List[dict]]: Metadata as Spark DataFrame or list of dicts.

        Raises:
            ValueError: If an unsupported return_type is specified.
        """
        print("📦 Fetching all database and table names...")
        databases = [db["databaseName"] for db in self.spark.sql("SHOW DATABASES").collect()]
        table_fqns = [
            (db, tbl["tableName"])
            for db in databases
            for tbl in self.spark.sql(f"SHOW TABLES IN `{db}`").collect()
        ]
        print(f"✅ Found {len(table_fqns)} tables across {len(databases)} schemas.")

        print("🔄 Extracting table metadata in parallel...")
        results = []
        lock = threading.Lock()

        def worker(fqn):
            row = self._extract_table_metadata(*fqn)
            with lock:
                results.append(row)

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            executor.map(worker, table_fqns)
        print("✅ Metadata extraction complete.")

        print("🧱 Converting results to Spark DataFrame...")
        df = self.spark.createDataFrame(results, schema=self.schema)

        if return_type == "dataframe":
            print("📊 Returning Spark DataFrame.")
            return df
        elif return_type == "json":
            print("📤 Returning metadata as list of dictionaries.")
            return [row.asDict() for row in df.collect()]
        else:
            raise ValueError(f"Invalid return_type: {return_type}. Must be 'dataframe' or 'json'")

# COMMAND ----------

# DBTITLE 1,Run Main Program
method = "json" # or "dataframe"

# Create the inspector
inspector = HiveMetastoreInspector(spark)

if method == "dataframe": # then return dataframe

  # Return as Spark DataFrame
  df = inspector.collect_metadata(return_type="dataframe")
  display(df)

else: # return json and write to dbfs/tmp

  json_result = inspector.collect_metadata(return_type="json")
  # Define output path (accessible from Databricks File System)
  output_path = "/dbfs/tmp/hive_metadata.json"
  # Write JSON to file
  with open(output_path, "w+") as f:
      json.dump(json_result, f)
  print(f"✅ JSON metadata written to: {output_path}")

# COMMAND ----------

# DBTITLE 1,Read Hive Metastore Metadata and Print Json
# Read the JSON file from DBFS
with open("/dbfs/tmp/hive_metadata.json", "r") as f:
    metadata = json.load(f)

# Preview the first few records
from pprint import pprint
pprint(metadata)  # or print(json.dumps(metadata[:2], indent=2))