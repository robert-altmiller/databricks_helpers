# Databricks notebook source
# DBTITLE 1,Python Imports
import os, time
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from logging_timing import CustomLoggerTimer
from helpers import *


# Initialize logger and timing functions
logger = CustomLoggerTimer(spark, "execution.log", refresh_data = True)

# COMMAND ----------

# DBTITLE 1,Execute Pandas UDF Example
@logger.timer
def generate_dynamic_udf(spark, n: int):

    total_running_executors = get_total_executors(spark=spark) # Executes on the Spark driver
    
    @pandas_udf("struct<os_pid:string, hostname:string, hostname_ip:string, total_running_executors:int, udf_start_time:string, udf_end_time:string, udf_duration:double, features:string>")
    def dynamic_transform_udf(series: pd.Series) -> pd.DataFrame:
        start_time = time.time()

        # Generate all features
        features = {
            f"feature_{i+1}": (
                np.log1p(series + i) +
                np.sin(series + i) +
                (series ** (i % 3 + 1))
            )
            for i in range(n)
        }

        # Compose final result — one row per input value
        result_df = pd.DataFrame({
            "os_pid": str(os.getpid()),
            "hostname": str(os.uname().nodename),
            "hostname_ip": str(get_external_ip()),
            "total_running_executors": total_running_executors,
            "udf_start_time": str(start_time),
            "udf_end_time": str(time.time()),
            "udf_duration": time.time() - start_time,
            "features": [str({f"feature_{i+1}": features[f"feature_{i+1}"].iloc[j] for i in range(n)}) for j in range(len(series))]
        })

        return result_df
    
    return dynamic_transform_udf


n = 1000  # Change this to however many transformed columns you want
transform_udf = generate_dynamic_udf(spark, n)
output_columns = [f"feature_{i+1}" for i in range(n)]

# Apply the transformation
#df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["value"])
df = spark.createDataFrame([(i,) for i in range(1, n+1)], ["value"])
df_transformed = df.select(transform_udf(df["value"]).alias("udf_data"), "value")

# COMMAND ----------

# DBTITLE 1,Print Spark Driver Processing Time
driver_data = logger.read_logging()
print(driver_data)

# COMMAND ----------

# DBTITLE 1,Print Spark Executors Processing Time
udf_schema = StructType() \
    .add("os_pid", StringType()).add("hostname", StringType()) \
    .add("hostname_ip", StringType()).add("total_running_executors", IntegerType()) \
    .add("udf_start_time", StringType()).add("udf_end_time", StringType()) \
    .add("udf_duration", DoubleType()).add("features", StringType()) 

df_parsed = df_transformed.withColumn("udf_struct", from_json(to_json(col("udf_data")), udf_schema))

df_parsed = df_parsed.select(
    "value",
    col("udf_struct.os_pid"), col("udf_struct.hostname"), 
    col("udf_struct.hostname_ip"), col("udf_struct.total_running_executors"), 
    col("udf_struct.udf_start_time"), col("udf_struct.udf_end_time"), 
    col("udf_struct.udf_duration"), col("udf_struct.features")
)
df_parsed = df_parsed \
    .withColumn("udf_start_time", from_unixtime(col("udf_start_time")).cast("timestamp")) \
    .withColumn("udf_end_time", from_unixtime(col("udf_end_time")).cast("timestamp"))

#df_parsed = df_parsed.select("udf_end_time", "udf_duration", "hostname", "hostname_ip").
display(df_parsed)

# COMMAND ----------

# DBTITLE 1,Plot the Executor Row Level Timing Data
import plotly.express as px

df_plot = df_parsed.toPandas()

# Optional: combine host and PID for a cleaner label
df_plot["executor_id"] = df_plot["hostname_ip"]

fig = px.line(
    df_plot,
    x="udf_end_time",
    y="udf_duration",
    color="executor_id",  # One line per executor
    markers=True,
    title="Row Level UDF Duration over Time by Executor",
    labels={
        "udf_end_time_ts": "UDF End Time",
        "udf_duration": "Duration (sec)",
        "executor_id": "Executor"
    }
)

fig.update_layout(xaxis_tickformat="%H:%M:%S", xaxis_title="End Time", yaxis_title="Row Level UDF Duration (seconds)")
fig.show()