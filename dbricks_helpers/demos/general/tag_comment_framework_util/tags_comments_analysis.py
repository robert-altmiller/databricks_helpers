# Databricks notebook source
# MAGIC %md
# MAGIC # Tags and Comments Analysis
# MAGIC
# MAGIC This notebook applies tags and generates AI-powered descriptions for Unity Catalog tables and columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper
import pandas as pd
import requests
import json
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import utilities
from utils import (
    ColumnDescriptionGenerator,
    ColumnDescriptionImporter,
    TableDescriptionGenerator,
    TableDescriptionImporter,
    TagProcessor
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# DBTITLE 1,Widgets
# Create widgets
dbutils.widgets.text("file_path", "/Volumes/dna_dev/raltmil/blobs/Tags_Comments_Analysis_sample_2.xlsx", "Excel File Path")
dbutils.widgets.text("base_volume_path", "/Volumes/dna_dev/raltmil/blobs", "Volume Path")
dbutils.widgets.dropdown("model_endpoint", "databricks-claude-sonnet-4-5", 
                        ["databricks-meta-llama-3-3-70b-instruct", "databricks-claude-sonnet-4-5", "databricks-gemini-2-5-pro"], 
                        "AI Model Endpoint")

# Get parameters
file_path = dbutils.widgets.get("file_path")
base_volume_path = dbutils.widgets.get("base_volume_path")
model_endpoint = dbutils.widgets.get("model_endpoint")

# Get workspace info
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Calculate parallelism
try: 
    default_parallelism = spark.sparkContext.defaultParallelism / 2
except: 
    default_parallelism = (4 * os.cpu_count()) - 1

print(f"Number of executors: {int(default_parallelism)}")
print(f"File path: {file_path}")
print(f"Volume path: {base_volume_path}")
print(f"AI Model Endpoint: {model_endpoint}")
print(f"\nWorkspace: {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Excel File

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

# Read Excel
try:
    df_pandas = pd.read_excel(file_path)
    print(f"Successfully read Excel file with {len(df_pandas)} rows")
    print("\nColumns found:")
    print(df_pandas.columns.tolist())
except Exception as e:
    print(f"Error reading Excel: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preprocessing

# COMMAND ----------

# Normalize column names
df_pandas.columns = df_pandas.columns.str.replace('\n', ' ').str.strip()
TAG_COLUMN_MAPPING = {}  # Will be populated dynamically
# Map columns to standardized names
column_mapping = {}
for col_name in df_pandas.columns:
    lower_col = col_name.lower()
    if 'table_catalog' in lower_col:
        column_mapping[col_name] = 'table_catalog'
    elif 'table_schema' in lower_col:
        column_mapping[col_name] = 'table_schema'
    elif 'table_name' in lower_col and 'tag' not in lower_col:
        column_mapping[col_name] = 'table_name'
    elif lower_col.startswith('tag_'):
        print(f"Mapping column '{col_name}' to '{lower_col}'")
        column_mapping[col_name] = 'tag_' + re.sub(r'[\{\}\[\]]', '', lower_col.split(":")[1].strip())
    elif 'add' in lower_col and 'description' in lower_col and 'table' in lower_col:
        column_mapping[col_name] = 'add_table_description'
    elif 'add' in lower_col and 'description' in lower_col and 'column' in lower_col:
        column_mapping[col_name] = 'add_column_description'

df_pandas = df_pandas.rename(columns=column_mapping)

# Auto-detect tag columns
TAG_COLUMN_MAPPING = {
    col: col.replace('tag_', '') 
    for col in df_pandas.columns 
    if col.startswith('tag_')
}

print(f"\nAuto-detected columns: {list(df_pandas.keys())}")
print(f"Tag keys to be applied: {list(TAG_COLUMN_MAPPING.values())}")

# Display preview
print("\nData preview:")
print(df_pandas[['table_catalog', 'table_schema', 'table_name']].head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Tags to Tables

# COMMAND ----------

# DBTITLE 1,Apply Tags with TagProcessor
print("=" * 80)
print("APPLYING TAGS TO TABLES")
print(f"Tag columns to process: {list(TAG_COLUMN_MAPPING.values())}")
print("=" * 80)

# Initialize TagProcessor
tag_processor = TagProcessor(
    spark=spark,
    tag_column_mapping=TAG_COLUMN_MAPPING,
    verbose=False
)

# Execute with multithreading
tag_results = tag_processor.execute(df_pandas, max_workers=int(default_parallelism))

print("\n" + "=" * 80)
print(f"TAGS APPLICATION COMPLETE: {len(tag_results)} tables processed")
print("=" * 80)

# Display summary
if tag_results:
    successful = sum(1 for r in tag_results if r['status'] in ['success', 'partial'])
    failed = sum(1 for r in tag_results if r['status'] == 'error')
    partial = sum(1 for r in tag_results if r['status'] == 'partial')
    
    print("\nSummary:")
    print(f"  Total: {len(tag_results)}")
    print(f"  Successful: {successful - partial}")
    print(f"  Partial: {partial}")
    print(f"  Failed: {failed}")
    
    # Show partial failure details
    if partial > 0:
        print("\n⚠️  Partial Success Details:")
        for r in tag_results:
            if r['status'] == 'partial':
                print(f"   {r['table']}")
                print(f"      ✓ Tags applied: {r['success_count']}")
                print(f"      ✗ Tags failed: {r['failed_count']}")
                for error in r.get('errors', []):
                    print(f"         - {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Tables Requiring Descriptions

# COMMAND ----------

# DBTITLE 1,Identify Tables Needing Descriptions
tables_needing_table_desc = []
tables_needing_column_desc = []
tables_needing_any_desc = []

for idx, row in df_pandas.iterrows():
    catalog = row.get('table_catalog')
    schema = row.get('table_schema')
    table = row.get('table_name')
    add_table_desc = row.get('add_table_description')
    add_column_desc = row.get('add_column_description')
    
    if pd.isna(catalog) or pd.isna(schema) or pd.isna(table):
        continue
    
    full_table_name = f"{catalog}.{schema}.{table}"
    
    if pd.notna(add_table_desc) and str(add_table_desc).strip().upper() == 'Y':
        tables_needing_table_desc.append(full_table_name)
        tables_needing_any_desc.append(full_table_name)
    
    if pd.notna(add_column_desc) and str(add_column_desc).strip().upper() == 'Y':
        tables_needing_column_desc.append(full_table_name)
        if full_table_name not in tables_needing_any_desc:
            tables_needing_any_desc.append(full_table_name)

print("\n" + "=" * 80)
print("DESCRIPTION PROCESSING SUMMARY")
print("=" * 80)
print(f"Total tables in Excel: {len(df_pandas)}")
print(f"Tables with tags applied: {len(tag_results)}")
print(f"Tables needing TABLE descriptions: {len(tables_needing_table_desc)}")
print(f"Tables needing COLUMN descriptions: {len(tables_needing_column_desc)}")
print(f"Total tables needing any descriptions: {len(tables_needing_any_desc)}")
print("=" * 80)

# Clean up old description files for fresh run
if tables_needing_column_desc or tables_needing_table_desc:
    print("\nCleaning old description files...")
    try:
        if tables_needing_column_desc:
            dbutils.fs.rm(f"{base_volume_path}/bulk_comments/columns/json", recurse=True)
            print("  ✓ Cleaned column description directory")
    except:
        pass
    
    try:
        if tables_needing_table_desc:
            dbutils.fs.rm(f"{base_volume_path}/bulk_comments/tables/json", recurse=True)
            print("  ✓ Cleaned table description directory")
    except:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Descriptions

# COMMAND ----------

# DBTITLE 1,Column Descriptions (Multithreaded)
if tables_needing_column_desc:
    print("\n" + "=" * 80)
    print("PROCESSING COLUMN DESCRIPTIONS (MULTITHREADED)")
    print("=" * 80)
    print(f"Tables to process: {len(tables_needing_column_desc)}")
    print(f"AI Model: {model_endpoint}")
    print(f"Parallelism level: {int(default_parallelism)}")
    
    # Configuration
    column_desc_config = {
        "endpoint_name": model_endpoint,
        "data_limit": 5,
        "max_cell_chars": 250,
        "always_update": True,
        "prompt_return_length": 40,
        "output_file_format": "json"
    }
    
    # Helper: Generate
    def generate_column_descriptions_for_table(table_full_name):
        try:
            parts = table_full_name.split('.')
            if len(parts) != 3:
                return {"table": table_full_name, "status": "error", "message": "Invalid table name format", "columns": 0}
            
            catalog, schema, table = parts
            generator = ColumnDescriptionGenerator(
                spark=spark,
                catalog=catalog,
                schema=schema,
                table=table,
                output_path=base_volume_path,
                **column_desc_config
            )
            
            commented_columns = generator.execute()
            column_count = commented_columns.count()
            generator.save_comments(commented_columns)
            
            return {"table": table_full_name, "status": "success", "message": f"{column_count} columns", "columns": column_count}
        except Exception as e:
            return {"table": table_full_name, "status": "error", "message": str(e)[:100], "columns": 0}
    
    # Helper: Import
    def import_column_descriptions_for_table(table_info):
        try:
            table_full_name = table_info["table"]
            parts = table_full_name.split('.')
            if len(parts) != 3:
                return {"table": table_full_name, "status": "error", "message": "Invalid table name", "columns": 0}
            
            catalog, schema, table = parts
            importer = ColumnDescriptionImporter(
                spark=spark,
                input_path=f"{base_volume_path}/bulk_comments/columns/json/{table}",
                output_file_format="json"
            )
            
            result = importer.execute()
            return {
                "table": table_full_name,
                "status": "success",
                "message": f"{result['successful_columns']} columns",
                "columns": result['successful_columns']
            }
        except Exception as e:
            return {"table": table_full_name, "status": "error", "message": str(e)[:100], "columns": 0}
    
    # Step 1: Generate
    print("\n--- Step 1: Generating Column Descriptions ---")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting parallel generation...")
    
    with ThreadPoolExecutor(max_workers=int(default_parallelism)) as executor:
        futures = {executor.submit(generate_column_descriptions_for_table, tbl): tbl for tbl in tables_needing_column_desc}
        generation_results = []
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            generation_results.append(result)
            status_icon = "✓" if result["status"] == "success" else "✗"
            print(f"  [{i}/{len(tables_needing_column_desc)}] {status_icon} {result['table']} - {result['message']}")
    
    successful_generations = [r for r in generation_results if r["status"] == "success"]
    total_columns = sum(r["columns"] for r in successful_generations)
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Generation Summary:")
    print(f"  ✓ Successful tables: {len(successful_generations)}/{len(tables_needing_column_desc)}")
    print(f"  ✓ Total columns generated: {total_columns}")
    
    # Step 2: Import
    print("\n--- Step 2: Importing Column Descriptions ---")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting parallel import...")
    
    with ThreadPoolExecutor(max_workers=int(default_parallelism)) as executor:
        futures = {executor.submit(import_column_descriptions_for_table, r): r["table"] for r in successful_generations}
        import_results = []
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            import_results.append(result)
            status_icon = "✓" if result["status"] == "success" else "✗"
            print(f"  [{i}/{len(successful_generations)}] {status_icon} {result['table']} - {result['message']}")
    
    successful_imports = [r for r in import_results if r["status"] == "success"]
    total_imported = sum(r["columns"] for r in successful_imports)
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Import Summary:")
    print(f"  ✓ Successful tables: {len(successful_imports)}/{len(successful_generations)}")
    print(f"  ✓ Total columns imported: {total_imported}")
    
    print("\n" + "=" * 80)
    print(f"COLUMN DESCRIPTIONS COMPLETE")
    print(f"  Tables processed: {len(successful_imports)}")
    print(f"  Columns updated: {total_imported}")
    print("=" * 80)
    
    # Track results
    column_desc_success_tables = [r["table"] for r in successful_imports]
    column_desc_failed_tables = [(r["table"], r["message"]) for r in import_results if r["status"] != "success"]
else:
    print("\n○ No tables need column descriptions - skipping")
    column_desc_success_tables = []
    column_desc_failed_tables = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Descriptions

# COMMAND ----------

# DBTITLE 1,Table Descriptions (Multithreaded)
if tables_needing_table_desc:
    print("\n" + "=" * 80)
    print("PROCESSING TABLE DESCRIPTIONS (MULTITHREADED)")
    print("=" * 80)
    print(f"Tables to process: {len(tables_needing_table_desc)}")
    print(f"AI Model: {model_endpoint}")
    print(f"Parallelism level: {int(default_parallelism)}")
    
    # Configuration
    table_desc_config = {
        "endpoint_name": model_endpoint,
        "data_limit": 2,
        "max_cell_chars": 1000,
        "always_update": True,
        "prompt_return_length": 200,
        "output_file_format": "json"
    }
    
    # Helper: Generate
    def generate_table_descriptions_for_table(table_full_name):
        try:
            parts = table_full_name.split('.')
            if len(parts) != 3:
                return {"table": table_full_name, "status": "error", "message": "Invalid table name format"}
            
            catalog, schema, table = parts
            generator = TableDescriptionGenerator(
                spark=spark,
                catalog=catalog,
                schema=schema,
                table=table,
                output_path=base_volume_path,
                **table_desc_config
            )
            
            try:
                table_descriptions = generator.execute()
            except Exception as gen_error:
                return {"table": table_full_name, "status": "error", "message": f"Generation: {str(gen_error)[:150]}"}
            
            try:
                generator.save_descriptions(table_descriptions)
            except Exception as save_error:
                return {"table": table_full_name, "status": "error", "message": f"Save: {str(save_error)[:150]}"}
            
            return {"table": table_full_name, "status": "success", "message": "Description generated"}
        except Exception as e:
            return {"table": table_full_name, "status": "error", "message": f"Error: {str(e)[:150]}"}
    
    # Step 1: Generate
    print("\n--- Step 1: Generating Table Descriptions ---")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting parallel generation...")
    
    with ThreadPoolExecutor(max_workers=int(default_parallelism)) as executor:
        futures = {executor.submit(generate_table_descriptions_for_table, tbl): tbl for tbl in tables_needing_table_desc}
        generation_results = []
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            generation_results.append(result)
            status_icon = "✓" if result["status"] == "success" else "✗"
            print(f"  [{i}/{len(tables_needing_table_desc)}] {status_icon} {result['table']} - {result['message']}")
    
    successful_generations = [r for r in generation_results if r["status"] == "success"]
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Generation Summary:")
    print(f"  ✓ Successful tables: {len(successful_generations)}/{len(tables_needing_table_desc)}")
    
    # Step 2: Import (batch) - only for successfully generated tables
    table_import_success = False
    if successful_generations:
        print("\n--- Step 2: Importing Table Descriptions ---")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting batch import...")
        
        try:
            # Get list of tables that were successfully generated
            tables_to_import = [r["table"] for r in successful_generations]
            
            importer = TableDescriptionImporter(
                spark=spark,
                input_path=f"{base_volume_path}/bulk_comments/tables/json",
                output_file_format="json",
                table_list=tables_to_import  # Only import these specific tables
            )
            result = importer.execute()
            print(f"  ✓ Imported descriptions for {result['tables_processed']} table(s)")
            table_import_success = (result['tables_processed'] > 0)
        except Exception as e:
            print(f"  ✗ Error importing table descriptions: {str(e)}")
            table_import_success = False
    
    print("\n" + "=" * 80)
    print(f"TABLE DESCRIPTIONS COMPLETE")
    print(f"  Tables processed: {len(successful_generations) if table_import_success else 0}")
    print("=" * 80)
    
    # Track results
    if table_import_success:
        table_desc_success_tables = [r["table"] for r in successful_generations]
        table_desc_failed_tables = [(r["table"], r.get("message", "Failed")) for r in generation_results if r["status"] != "success"]
    else:
        table_desc_success_tables = []
        failed_gen = [(r["table"], r.get("message", "Failed")) for r in generation_results if r["status"] != "success"]
        failed_import = [(r["table"], "Import failed") for r in successful_generations]
        table_desc_failed_tables = failed_gen + failed_import
else:
    print("\n○ No tables need table descriptions - skipping")
    table_desc_success_tables = []
    table_desc_failed_tables = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Execution Report

# COMMAND ----------

# DBTITLE 1,Execution Summary
# Calculate summary
successful_tags = sum(1 for r in tag_results if r['status'] in ['success', 'partial']) if tag_results else 0
failed_tags = sum(1 for r in tag_results if r['status'] == 'error') if tag_results else 0
tag_success_rate = (successful_tags / len(tag_results) * 100) if tag_results else 0

col_desc_success = len(column_desc_success_tables)
col_desc_failed = len(column_desc_failed_tables)

table_desc_success = len(table_desc_success_tables)
table_desc_failed = len(table_desc_failed_tables)

# Print summary
print("=" * 80)
print("📊 EXECUTION SUMMARY")
print("=" * 80)
print(f"\n📋 Tables Processed: {len(df_pandas)}")
print(f"\n🏷️  Tags:")
print(f"   ✓ Success: {successful_tags}")
print(f"   ✗ Failed: {failed_tags}")
print(f"   📈 Success Rate: {tag_success_rate:.1f}%")

print(f"\n📝 Column Descriptions:")
print(f"   ✓ Success: {col_desc_success}")
print(f"   ✗ Failed: {col_desc_failed}")

print(f"\n📄 Table Descriptions:")
print(f"   ✓ Success: {table_desc_success}")
print(f"   ✗ Failed: {table_desc_failed}")

# Show error details
if failed_tags > 0:
    print("\n⚠️  Tag Failures - Details:")
    for r in tag_results:
        if r['status'] == 'error':
            print(f"   ✗ {r['table']}")
            for error in r.get('errors', []):
                print(f"      {error}")

if col_desc_failed > 0:
    print("\n⚠️  Column Description Failures - Details:")
    for table, error_msg in column_desc_failed_tables:
        print(f"   ✗ {table}")
        print(f"      {error_msg}")

if table_desc_failed > 0:
    print("\n⚠️  Table Description Failures - Details:")
    for table, error_msg in table_desc_failed_tables:
        print(f"   ✗ {table}")
        print(f"      {error_msg}")

print("\n" + "=" * 80)

# Create detailed status table
detail_records = []
for idx, row in df_pandas.iterrows():
    catalog = row.get('table_catalog')
    schema = row.get('table_schema')
    table = row.get('table_name')
    
    if pd.isna(catalog) or pd.isna(schema) or pd.isna(table):
        continue
    
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Get statuses
    tag_result = next((r for r in tag_results if r['table'] == full_table_name), None)
    if tag_result:
        if tag_result['status'] == 'success':
            tag_status = "✓ Success"
        elif tag_result['status'] == 'partial':
            tag_status = "⚠ Partial"
        else:
            tag_status = "✗ Failed"
    else:
        tag_status = "○ Not Processed"
    
    # Column description status
    if full_table_name in column_desc_success_tables:
        col_desc_status = "✓ Success"
    elif full_table_name in [t[0] for t in column_desc_failed_tables]:
        col_desc_status = "✗ Failed"
    elif full_table_name in tables_needing_column_desc:
        col_desc_status = "✗ Not Processed"
    else:
        col_desc_status = "- Not Needed"
    
    # Table description status
    if full_table_name in table_desc_success_tables:
        table_desc_status = "✓ Success"
    elif full_table_name in [t[0] for t in table_desc_failed_tables]:
        table_desc_status = "✗ Failed"
    elif full_table_name in tables_needing_table_desc:
        table_desc_status = "✗ Not Processed"
    else:
        table_desc_status = "- Not Needed"
    
    detail_records.append({
        "Table": f"{catalog}.{schema}.{table}",
        "Tags": tag_status,
        "Table Desc": table_desc_status,
        "Column Desc": col_desc_status
    })

# Display detailed table
detail_df = pd.DataFrame(detail_records)
print("\n📋 Detailed Status:")
display(detail_df)

# Validation: Check for pending items
col_desc_pending = [t for t in tables_needing_column_desc if t not in column_desc_success_tables and t not in [x[0] for x in column_desc_failed_tables]]
table_desc_pending = [t for t in tables_needing_table_desc if t not in table_desc_success_tables and t not in [x[0] for x in table_desc_failed_tables]]

if col_desc_pending or table_desc_pending:
    print("\n⚠️  WARNING: Some items left in pending state!")
    if col_desc_pending:
        print(f"   Column descriptions pending: {len(col_desc_pending)}")
        for table in col_desc_pending:
            print(f"     - {table}")
    if table_desc_pending:
        print(f"   Table descriptions pending: {len(table_desc_pending)}")
        for table in table_desc_pending:
            print(f"     - {table}")

# Final status
overall_success = (successful_tags == len(tag_results)) and (col_desc_failed == 0) and (table_desc_failed == 0) and not col_desc_pending and not table_desc_pending
print("\n" + "=" * 80)
if overall_success:
    print("✅ ALL OPERATIONS COMPLETED SUCCESSFULLY")
else:
    print("⚠️  COMPLETED WITH SOME FAILURES - Check details above")
print("=" * 80)


# COMMAND ----------

