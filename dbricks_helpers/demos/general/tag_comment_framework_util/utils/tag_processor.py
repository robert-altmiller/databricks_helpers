"""
Tag Processor
"""

from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


class TagProcessor:
    """Apply tags to Unity Catalog tables."""
    
    def __init__(self, spark, tag_column_mapping: Dict[str, str], verbose=False):
        self.spark = spark
        self.tag_column_mapping = tag_column_mapping
        self.verbose = verbose
    
    def apply_tags_to_table(self, catalog: str, schema: str, table: str, tags: Dict[str, str]) -> Dict:
        """Apply tags to a single table."""
        full_table_name = f"{catalog}.{schema}.{table}"
        results = {"success": [], "failed": []}
        
        for tag_name, tag_value in tags.items():
            # Skip NA, NaN, None, empty, and null-like values
            if pd.isna(tag_value) or not tag_value or str(tag_value).strip().upper() in ['NA', 'NULL', '', 'NAN', 'N/A', 'NONE']:
                continue
            
            # Convert boolean-like values
            if str(tag_value) in ['1', '1.0']:
                tag_value = 'true'
            elif str(tag_value) in ['0', '0.0']:
                tag_value = 'false'
            
            try:
                self.spark.sql(f"ALTER TABLE {full_table_name} SET TAGS ('{tag_name}' = '{tag_value}')")
                results["success"].append(tag_name)
            except Exception as e:
                error_msg = str(e)[:150]
                results["failed"].append({"tag": tag_name, "error": error_msg})
                if self.verbose:
                    print(f"  ✗ Failed to set {tag_name}={tag_value}: {error_msg}")
        
        return results
    
    def process_single_table(self, row) -> Dict:
        """Process tags for a single table."""
        catalog = row["table_catalog"]
        schema = row["table_schema"]
        table = row["table_name"]
        full_name = f"{catalog}.{schema}.{table}"
        
        try:
            # Build tags dictionary from row
            tags = {}
            for col_name, tag_name in self.tag_column_mapping.items():
                if col_name in row and row[col_name]:
                    tags[tag_name] = str(row[col_name])
            
            # Apply tags
            results = self.apply_tags_to_table(catalog, schema, table, tags)
            
            if results["failed"]:
                failed_tags = [f"{f['tag']}: {f['error']}" for f in results["failed"]]
                return {
                    "table": full_name,
                    "status": "partial",
                    "success_count": len(results["success"]),
                    "failed_count": len(results["failed"]),
                    "errors": failed_tags
                }
            else:
                return {
                    "table": full_name,
                    "status": "success",
                    "success_count": len(results["success"]),
                    "failed_count": 0,
                    "errors": []
                }
        
        except Exception as e:
            return {
                "table": full_name,
                "status": "error",
                "success_count": 0,
                "failed_count": 0,
                "errors": [str(e)[:150]]
            }
    
    def execute(self, df, max_workers: int = 10) -> List[Dict]:
        """Execute tag application with multithreading."""
        tables = df.to_dict('records')
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.process_single_table, row): row for row in tables}
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
        
        return results
