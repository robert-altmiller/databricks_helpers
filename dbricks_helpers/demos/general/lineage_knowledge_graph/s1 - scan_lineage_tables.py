# Databricks notebook source
# DBTITLE 1,Install Libraries
import traceback, time, re
import pandas as pd
import numpy as np
from pyspark.sql import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, Row
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,User Parameters
# Workspace url
workspace_url = str(spark.conf.get("spark.databricks.workspaceUrl"))

# Catalog and schema for writing lineage data
catalog = "dna_dev"
schema = "raltmil"
lineage_tables_columns = "lineage_tables_columns_data" # Don't Modify
lineage_upstream_jobs = "lineage_upstream_jobs"        # Don't Modify
lineage_tables_base = "lineage_tables_base"            # Don't Modify
lineage_tables_combined = "lineage_tables_combined"    # Don't Modify

# Target table to get upstream and downstream lineage for
target = "gdp_pii_prod.customer360.member_summary"
source_catalogs = ["gdp_prod"] # IMPORTANT
#["gdp_pii_prod", "gdp_pii_preprod", "gdp_prod", "gdp_preprod", "dna_dev", "dna_stage", "dna_prod",  "gdp_stage", "main", "mlops_gdp_prod", "mlops_gdp_stage"]
source_catalogs_sql = ", ".join([f"'{c}'" for c in source_catalogs])
target_catalogs = ["gdp_prod"] # IMPORTANT
target_catalogs_sql = ", ".join([f"'{c}'" for c in target_catalogs])

# Number of months to go back to define recent data
days = 30

# How many level upstream to traverse
max_depth = 100

# # Output path for index.html node edge graph
# output_path = "/Workspace/Users/oedenfi@gap.com/index.html"

# Delete existing data
delete_existing_data = False # Don't Modify

# Create graph mode - "centroid_table" or "all_tables"
# Centroid table uses a table and creates a subset of lineage for that single table with recursion.  An important note of this type of run is that every source node will be connected to target node with an edge between them.
# All tables simply creates lineage for all tables in all specified catalogs.  An important note of this type of run is the graph will have standalone nodes, and subsets of nodes and edges living all on their own.
graph_mode = "all_tables" # centroid_table or all_tables

# COMMAND ----------

# DBTITLE 1,Delete Existing Data (Optional)
if delete_existing_data == True:
  spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{lineage_tables_columns}")
  spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{lineage_tables_base}")
  spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{lineage_tables_combined}")

# COMMAND ----------

# DBTITLE 1,Get Source Table Lineage
# ============================================================
# Skip internal / temporary / non-human tables
# ============================================================
def should_skip_table(name: str) -> bool:
    if name is None:
        return True
    name = name.strip()
    lname = name.lower()
    # ------------------------------------------------------------
    # 1. Random internal temp tables (e.g., t5af7uephmd001)
    # ------------------------------------------------------------
    if re.match(r"^[a-z][a-z0-9]{10,16}$", name):
        return True
    # ------------------------------------------------------------
    # 2. DLT / MLflow / DSFF auto-generated materializations
    # ------------------------------------------------------------
    if "_materialization" in lname:
        return True
    # ------------------------------------------------------------
    # 3. "inprocess" temporary tables with numeric suffix
    #    Examples:
    #      email_matcher_ir_inprocess_25885
    #      something_inprocess_123
    # ------------------------------------------------------------
    if re.search(r"_inprocess_\d+$", lname):
        return True
    return False
skip_udf = F.udf(should_skip_table, "boolean")

# ============================================================
# Databricks Lineage UC Table Enrichment
# ============================================================

# ---- Load raw column lineage --------------------------------
column_lineage = (
    spark.read.table("system.access.table_lineage")
    .select(
        "source_table_catalog",
        "source_table_schema",
        "source_table_name",
        F.coalesce("source_table_full_name", "source_path").alias("source_table_full_name"),
        "target_table_catalog",
        "target_table_schema",
        "target_table_name",
        F.coalesce("target_table_full_name", "target_path").alias("target_table_full_name"),
        "source_type",
        "target_type"
        # "entity_type",
        # "entity_run_id",
        # "event_time"
    )
    .where("source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL")
    .where(f"event_time >= current_date() - INTERVAL {days} DAYS")
    .where(F.col("source_table_catalog").isin(source_catalogs))
    .where(F.col("target_table_catalog").isin(target_catalogs))
    .distinct()
)

# ============================================================
# Extract table names only (3rd part of catalog.schema.table)
# ============================================================
column_lineage = (
    column_lineage
    .filter(~skip_udf(F.col("source_table_name")))
    .filter(~skip_udf(F.col("target_table_name")))
)

print(f"✅ Loaded table lineage: {column_lineage.count():,} rows")
display(column_lineage)

# COMMAND ----------

# DBTITLE 1,Get Distinct List of Column For Lineage Tables
# ============================================================
# Collect distinct tables from lineage
# ============================================================
src_df = (
    column_lineage
    .select(
        F.split("source_table_full_name", r"\.")[0].alias("catalog"),
        F.split("source_table_full_name", r"\.")[1].alias("schema"),
        F.split("source_table_full_name", r"\.")[2].alias("table")
    )
)
tgt_df = (
    column_lineage
    .select(
        F.split("target_table_full_name", r"\.")[0].alias("catalog"),
        F.split("target_table_full_name", r"\.")[1].alias("schema"),
        F.split("target_table_full_name", r"\.")[2].alias("table")
    )
)
distinct_tables = (
    src_df.union(tgt_df)
          .distinct()
          .collect()
)

print(f"📚 Found {len(distinct_tables):,} unique tables to fetch UC columns for...")
# print(distinct_tables)

# COMMAND ----------

# DBTITLE 1,Get UC Columns For Each Lineage Table and Write Table
# ============================================================
# Helper: if lineage_tables_columns exists get loaded tables
# ============================================================

try: # check if lineage_tables_columns exists
    lineage_table_cols_loaded_list = [
        r["full_table_name"]
        for r in spark.read.table(f"{catalog}.{schema}.{lineage_tables_columns}")
        .select("full_table_name")
        .collect()
    ]
except:
    lineage_table_cols_loaded_list = []

# ============================================================
# Fetch UC columns (returns Pandas DF, safe for threads)
# ============================================================
def get_uc_columns_pd(_catalog, _schema, _table):
    full_name = f"{_catalog}.{_schema}.{_table}"
    try:
        print(f"🔍 Fetching {full_name} ...", flush=True)
        cols = (
            spark.read.table("system.information_schema.columns")
            .filter(
                (F.col("table_catalog") == _catalog)
                & (F.col("table_schema") == _schema)
                & (F.col("table_name") == _table)
            )
            .select("column_name")
            .toPandas()  # collect inside thread!
        )

        if cols.empty:
            print(f"⚠️ No columns found for {full_name}", flush=True)
            return None

        return pd.DataFrame({
            "full_table_name": [full_name],
            "uc_columns": [cols["column_name"].unique().tolist()]
        })

    except Exception as e:
        print(f"⚠️ Skipped {full_name}: {e}", flush=True)
        return None

# ============================================================
# Thread pool + batch append
# ============================================================
max_workers = 1
batch_size = 10
pause_between = 1

batches_written = 0
processed = 0
uc_parts = []

# Get a list of tables to fetch UC columns for and skip already loaded tables
distinct_tables_list = [f"{r.catalog}.{r.schema}.{r.table}" for r in distinct_tables]
tables_not_loaded_list = list(set(distinct_tables_list) - set(lineage_table_cols_loaded_list))
print(f"tables_not_loaded_list (e.g., columns not captured count): {len(tables_not_loaded_list)}")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(get_uc_columns_pd, *t.split(".", 2)): t for t in tables_not_loaded_list}
    time.sleep(pause_between)

    for future in as_completed(futures):
        processed += 1
        result_pd = future.result()

        if result_pd is not None and not result_pd.empty:
            uc_parts.append(result_pd)

            print(f"📦 buffered={len(uc_parts)} processed={processed}", flush=True)

            if len(uc_parts) >= batch_size:
                batches_written += 1
                print(f"🧩 Writing batch {batches_written} ({len(uc_parts)} tables)...", flush=True)

                # Convert Pandas list → Spark DF
                combined_pd = pd.concat(uc_parts, ignore_index=True)
                combined_spark = spark.createDataFrame(combined_pd)

                (
                    combined_spark.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{catalog}.{schema}.{lineage_tables_columns}")
                )

                print(f"✅ Batch {batches_written} written", flush=True)
                uc_parts.clear()

# Final flush
if uc_parts:
    batches_written += 1
    print(f"🧩 Writing final batch {batches_written} ({len(uc_parts)} tables)...", flush=True)
    combined_pd = pd.concat(uc_parts, ignore_index=True)
    combined_spark = spark.createDataFrame(combined_pd)
    (
        combined_spark.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{lineage_tables_columns}")
    )
    print(f"✅ Final batch {batches_written} written", flush=True)

print(f"\n✅ Done. Processed {processed:,} tables total, {batches_written} batches written.")


# COMMAND ----------

# DBTITLE 1,Enrich Lineage Table With UC Columns and Write Table
# Read lineage_tables_columns table
lineage_cols_df = spark.read.table(f"{catalog}.{schema}.{lineage_tables_columns}")

# Broadcast the small lookup DataFrame (columns_df)
columns_b = F.broadcast(lineage_cols_df)

# Pre-alias the columns once instead of twice (avoids redundant renames)
columns_source = columns_b.selectExpr(
    "full_table_name as source_table_full_name",
    "uc_columns as source_uc_columns"
    
)
columns_target = columns_b.selectExpr(
    "full_table_name as target_table_full_name",
    "uc_columns as target_uc_columns"
)

# Optional safety: ensure source_columns/target_columns exist
for c in ["source_columns", "target_columns"]:
    if c not in column_lineage.columns:
        column_lineage = column_lineage.withColumn(c, F.array().cast(T.ArrayType(T.StringType())))

# Single-pass join with broadcast + simplified logic
enriched_lineage = (
    column_lineage
    .join(columns_source, on="source_table_full_name", how="left")
    .join(columns_target, on="target_table_full_name", how="left")
    .withColumn("source_columns", F.array_distinct(F.col("source_uc_columns")))
    .withColumn("target_columns", F.array_distinct(F.col("target_uc_columns")))
    .drop("source_uc_columns", "target_uc_columns")
)

# ============================================================
# Write enriched result
# ============================================================
if graph_mode == "all_tables":
    write_table_name = lineage_tables_combined
else: write_table_name = lineage_tables_base

(
    enriched_lineage.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{write_table_name}")
)

print(f"✅ Enriched lineage merged: {enriched_lineage.count():,} rows")
print(f"✅ Enriched lineage table '{catalog}.{schema}.{write_table_name}' written successfully!")
#display(enriched_lineage)

# COMMAND ----------

# DBTITLE 1,Create New Feature for Upstream Job Dependencies and Write Table
# Read all table names from your lineage_tables_columns_data table and collect them to the driver
lineage_tables_columns_df = spark.sql(f"SELECT DISTINCT full_table_name FROM {catalog}.{schema}.{lineage_tables_columns}").collect()
# Convert the collected rows into a unique Python list of table names
lineage_tables_list = list(set([row.full_table_name for row in lineage_tables_columns_df]))
# Query system.access.column_lineage to find jobs that wrote to any of those upstream tables
lineage_jobs_df = spark.sql(f"""
  SELECT DISTINCT 
      workspace_id, 
      entity_id as job_id, 
      target_table_full_name
  FROM system.access.column_lineage
  WHERE target_table_full_name IN ({', '.join([repr(name) for name in lineage_tables_list])})
    AND entity_type = 'JOB'                                  -- only include job-related lineage
    AND event_time > current_date() - INTERVAL {days} DAYS   -- only recent lineage
""")
# Register as a temporary view so it can be referenced in later SQL
lineage_jobs_df.createOrReplaceTempView("lineage_jobs_temp")

# ---------------------------------------
# Build SQL that finds:
# 1) The most recent version of each job (recent_jobs)
# 2) The most recent *successful* run of each job within days (recent_runs)
# 3) Joins that to lineage_jobs_temp to see which jobs touch which tables
# ---------------------------------------
SQL = f"""
  -- Pull the latest metadata for each job (name, change_time)
  WITH recent_jobs AS (
    SELECT 
        workspace_id,
        job_id,
        name,
        change_time
    FROM system.lakeflow.jobs
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, job_id
        ORDER BY change_time DESC
    ) = 1  -- keep only the newest job definition per job_id
  ),

  -- Pull the most recent successful run per job inside the time window
  recent_runs AS (
    SELECT
        workspace_id,
        job_id,
        result_state,
        period_start_time
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time > current_date() - INTERVAL {days} DAYS
      AND result_state = 'SUCCEEDED'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, job_id
        ORDER BY period_start_time DESC
    ) = 1  -- keep the latest successful run
  )

  -- Main SELECT:
  -- Join lineage (table → job), job metadata, job runs, and workspace info
  SELECT 
      w.workspace_name,
      w.workspace_url,
      lj.target_table_full_name,
      j.job_id,
      j.name,
      CONCAT(w.workspace_url, '/jobs/', j.job_id) AS job_url
  FROM lineage_jobs_temp lj
  JOIN recent_jobs j
    ON j.workspace_id = lj.workspace_id
   AND j.job_id = lj.job_id
  JOIN recent_runs r
    ON r.workspace_id = lj.workspace_id
   AND r.job_id = lj.job_id
  JOIN system.access.workspaces_latest w
    ON w.workspace_id = lj.workspace_id
  ORDER BY j.change_time DESC
"""

# Execute SQL
lineage_upstream_jobs_df = spark.sql(SQL)

# ---------------------------------------
# Aggregate into one row per table
# Create a list of jobs = [{workspace_name, job_name, job_url}, ...]
# ---------------------------------------
lineage_upstream_jobs_agg_df = (
    lineage_upstream_jobs_df
    .groupBy(
        "target_table_full_name"
    )
    .agg(
        F.collect_list(
            F.struct(
                F.col("name").alias("job_name"),
                F.col("job_url")
            )
        ).alias("jobs")
    )
)

# Create Upstream Dependency Jobs for knowledge graph
(
    lineage_upstream_jobs_agg_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{lineage_upstream_jobs}")
)

print(f"✅ Lineage jobs data written to {catalog}.{schema}.{lineage_upstream_jobs}")
# display(lineage_upstream_jobs_agg_df)

# COMMAND ----------

# DBTITLE 1,Create Nodes and Edges Mapping
def traverse_lineage(base_lineage_df, target, max_depth=2, spark=None):
    """
    Recursively traverses lineage from a target table up to a specified depth,
    preserving attributes like entity_type, entity_run_id, and column arrays.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    # --- Step 1: Convert to Pandas safely ---
    lineage_pd = base_lineage_df.select(
        "source_table_full_name",
        "target_table_full_name",
        # "entity_type",
        # "entity_run_id",
        "source_type",
        "target_type",
        "source_columns",
        "target_columns"
    ).toPandas()

    # Ensure all arrays are plain Python lists (not NumPy arrays)
    lineage_pd["source_columns"] = lineage_pd["source_columns"].apply(
        lambda x: list(x) if isinstance(x, (list, np.ndarray)) else []
    )
    lineage_pd["target_columns"] = lineage_pd["target_columns"].apply(
        lambda x: list(x) if isinstance(x, (list, np.ndarray)) else []
    )

    # --- Step 2: Build adjacency dictionary ---
    adjacency = {}
    for _, row in lineage_pd.iterrows():
        tgt, src = row["target_table_full_name"], row["source_table_full_name"]
        if not tgt or not src:
            continue

        edge_meta = {
            "source_table_full_name": src,
            "target_table_full_name": tgt,
            # "entity_type": row["entity_type"],
            # "entity_run_id": row["entity_run_id"],
            "source_type": row["source_type"],
            "target_type": row["target_type"],
            "source_columns": row["source_columns"],
            "target_columns": row["target_columns"]
        }
        adjacency.setdefault(tgt, []).append(edge_meta)

    print(f"✅ Adjacency map built with {len(adjacency):,} targets")

    # --- Step 3: BFS traversal ---
    queue = deque([(target, 0)])
    seen = {target}
    all_edges = []

    while queue:
        current_target, depth = queue.popleft()
        if depth >= max_depth:
            continue
        for edge_meta in adjacency.get(current_target, []):
            src = edge_meta["source_table_full_name"]
            if not src:
                continue
            all_edges.append(edge_meta)
            if src not in seen:
                seen.add(src)
                queue.append((src, depth + 1))

    print(f"✅ Traversal complete up to {max_depth} levels: {len(all_edges):,} edges, {len(seen):,} nodes")

    # --- Step 4: Define schema explicitly ---
    schema = T.StructType([
        T.StructField("source_table_full_name", T.StringType()),
        T.StructField("target_table_full_name", T.StringType()),
        # T.StructField("entity_type", T.StringType()),
        # T.StructField("entity_run_id", T.StringType()),
        T.StructField("source_type", T.StringType()),
        T.StructField("target_type", T.StringType()),
        T.StructField("source_columns", T.ArrayType(T.StringType())),
        T.StructField("target_columns", T.ArrayType(T.StringType()))
    ])

    # --- Step 5: Create DataFrame safely ---
    lineage_df = spark.createDataFrame([Row(**edge) for edge in all_edges], schema=schema)
    return lineage_df.orderBy("target_table_full_name")


if graph_mode == "centroid_table":
    # Example run
    lineage_result_df = traverse_lineage(
        base_lineage_df=spark.read.table(f"{catalog}.{schema}.{lineage_tables_base}"),
        target=target,
        max_depth=max_depth,
        spark=spark
    )

    # Create lineage table for knowledge graph
    lineage_result_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.{lineage_tables_combined}")

    print(f"✅ Lineage data written to {catalog}.{schema}.{lineage_tables_combined}")
    display(lineage_result_df)