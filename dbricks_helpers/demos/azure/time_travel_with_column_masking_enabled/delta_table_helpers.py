# Databricks notebook source
# DBTITLE 1,Show Delta Table History
def show_delta_table_history(table_fqn: str):
    """
    Displays the version history of a Delta table using Delta Lake's history API.
    Parameters:
        table_fqn (str): Fully qualified name of the Delta table (e.g., 'main.pii_demo.synthetic_pii').
    Returns:
        None. Displays the history as a DataFrame in the notebook UI.
    """
    # Load the Delta table
    delta_table = DeltaTable.forName(spark, table_fqn)
    # Fetch the full version history
    history_df = delta_table.history().orderBy("version", ascending=False)
    # Display the history DataFrame
    display(history_df, truncate=False)
    return history_df

# COMMAND ----------

def read_delta_table_version(table_fqn: str, version: int) -> DataFrame:
    """
    Reads data from a specific version of a Delta table using Delta Lake time travel.
    Parameters:
        table_fqn (str): Fully qualified Delta table name (e.g., 'main.pii_demo.synthetic_pii').
        version (int): The version number to read.
    Returns:
        DataFrame: Spark DataFrame containing the data at the specified version.
    """
    df = spark.read.format("delta").option("versionAsOf", version).table(table_fqn)
    return df