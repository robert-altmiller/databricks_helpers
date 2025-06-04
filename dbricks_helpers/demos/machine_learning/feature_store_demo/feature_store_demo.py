# Databricks notebook source
# MAGIC %md
# MAGIC ## Feature store - example for travel recommendation
# MAGIC <a href="https://docs.databricks.com/en/machine-learning/feature-store/uc/feature-tables-uc.html" target="_blank">Feature Engineering in Databricks Unity Catalog</a> allows you to create a centralized repository of features. These features can be used to train and call your ML models. By saving features as feature engineering tables in Unity Catalog, you will be able to:
# MAGIC
# MAGIC - Share features across your organization 
# MAGIC - Increase discoverability 
# MAGIC - Ensure that the same feature computation code is used for model training and inference
# MAGIC - Leverage your Delta Lake tables for batch inference
# MAGIC
# MAGIC With Serverless, you can also enable a real-time backend and use a Key-Value store for realtime inference, and create a feature spec endpoint to compute inference features in realtime.
# MAGIC
# MAGIC **We'll go in detail on how to:**
# MAGIC  - Batch ingest and save our data as a feature table within Unity Catalog
# MAGIC  - Create Feature Store tables with streaming
# MAGIC  - Create a Feature Lookup and join multiple Feature Store tables
# MAGIC  - Do point in time lookups
# MAGIC  - Save a Feature Spec (with functions) to UC 
# MAGIC  - Train your model using the Feature Engineering Client
# MAGIC  - Register your model and promote it to production
# MAGIC  - Perform batch scoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building a propensity score to book travels & hotels
# MAGIC
# MAGIC Fot this demo, we'll step in the shoes of a Travel Agency offering deals in their website.
# MAGIC
# MAGIC Our job is to increase our revenue by boosting the amount of purchases, pushing personalized offer based on what our customers are the most likely to buy.
# MAGIC
# MAGIC In order to personalize offer recommendation in our application, we have have been asked as a Data Scientist to create the TraveRecommendationModel that predicts the probability of purchasing a given travel. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

import os
import time

import mlflow
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.window as w
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler

from databricks.feature_engineering import (
    FeatureEngineeringClient,
    FeatureFunction,
    FeatureLookup,
)

# COMMAND ----------

catalog = "188_edap_dev"
schema = "feature"

# COMMAND ----------

spark.sql(f"USE `{catalog}`.`{schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initial data setup

# COMMAND ----------

purchase_logs_path = "/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_vacation-purchase_logs/"

(
  spark.read.option("inferSchema", "true")
  .load(purchase_logs_path, format="csv", header="true")
  .withColumn("id", F.monotonically_increasing_id())
  .withColumn("booking_date", F.col("booking_date").cast("date"))
  .write.mode("overwrite")
  .saveAsTable(f"{catalog}.{schema}.demo_travel_purchase")
)

# COMMAND ----------

destination_location_path = "/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-locations/"

(
  spark.read.option("inferSchema", "true")
  .load(destination_location_path, format="csv", header="true")
  .write.mode("overwrite")
  .saveAsTable(f"{catalog}.{schema}.demo_destination_location")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create feature tables
# MAGIC
# MAGIC Our first step is to create Feature Store tables.
# MAGIC
# MAGIC We will load data from the silver table `travel_purchase` and create two feature tables:
# MAGIC * **User features**: contains all the features for a given user in a given point in time (location, previous purchases if any etc)
# MAGIC * **Destination features**: data on the user's travel destination for a given point in time (interest tracked by the number of clicks & impression)
# MAGIC
# MAGIC Data from the `destination_location` table will also be used.
# MAGIC
# MAGIC We'll consume availability data through streaming, making sure the table is refreshed in near realtime.
# MAGIC
# MAGIC In addition, we'll compute the "on-demande" feature (distance between the user and a destination, booking time) using the pandas API during training, this will allow us to use the same code for realtime inferences.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-flow-training.png?raw=true" width="1200px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Point-in-time support for feature tables
# MAGIC
# MAGIC Databricks Feature Store supports use cases that require point-in-time correctness.
# MAGIC
# MAGIC The data used to train a model often has time dependencies built into it. In our case, because we are adding rolling-window features, our Feature Table will contain data on all the dataset timeframe. 
# MAGIC
# MAGIC When we build our model, we must consider only feature values up until the time of the observed target value. If we do not explicitly take into account the timestamp of each observation, we might inadvertently use feature values measured after the timestamp of the target value for training. This is called “data leakage” and can negatively affect the model’s performance.
# MAGIC
# MAGIC <img src="https://docs.databricks.com/aws/en/assets/images/point-in-time-overview-e5699d6917724a99703d41f544ac98b6.png" width="600px"/>
# MAGIC
# MAGIC Time series feature tables include a timestamp key column that ensures that each row in the training dataset represents the latest known feature values as of the row’s timestamp. 
# MAGIC
# MAGIC In our case, this timestamp key will be the `ts` field, present in our 3 feature tables.
# MAGIC
# MAGIC See [documentation](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series) for more information

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compute batch features
# MAGIC
# MAGIC Calculate the aggregated features from the vacation purchase logs for destination and users. The destination features include popularity features such as impressions, clicks, and pricing features like price at the time of booking. The user features capture the user profile information such as past purchased price. Because the booking data does not change very often, it can be computed once per day in batch.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_travel_purchase limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_destination_location limit 3

# COMMAND ----------

def create_user_features(travel_purchase_df):
    travel_purchase_df = travel_purchase_df.withColumn('ts_l', F.col("ts").cast("long"))
    travel_purchase_df = (
        # Sum total purchased for 7 days
        travel_purchase_df.withColumn("lookedup_price_7d_rolling_sum",
            F.sum("price").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(7 * 86400), end=0))
        )
        # counting number of purchases per week
        .withColumn("lookups_7d_rolling_sum", 
            F.count("*").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(7 * 86400), end=0))
        )
        # total price 7d / total purchases for 7 d 
        .withColumn("mean_price_7d",  F.col("lookedup_price_7d_rolling_sum") / F.col("lookups_7d_rolling_sum"))
         # converting True / False into 1/0
        .withColumn("tickets_purchased", F.col("purchased").cast('int'))
        # how many purchases for the past 6m
        .withColumn("last_6m_purchases", 
            F.sum("tickets_purchased").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(6 * 30 * 86400), end=0))
        )
        .select("user_id", "ts", "mean_price_7d", "last_6m_purchases", "user_longitude", "user_latitude")
    )
    return travel_purchase_df
  

def create_destination_features(travel_purchase_df):
  return (
      travel_purchase_df
        .withColumn("clicked", F.col("clicked").cast("int"))
        .withColumn("sum_clicks_7d", 
          F.sum("clicked").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
        )
        .withColumn("sum_impressions_7d", 
          F.count("*").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
        )
        .select("destination_id", "ts", "sum_clicks_7d", "sum_impressions_7d")
  )

# COMMAND ----------

fe = FeatureEngineeringClient(model_registry_uri="databricks-uc")

user_features_df = create_user_features(spark.table(f"{catalog}.{schema}.demo_travel_purchase"))
fe.create_table(name=f"{catalog}.{schema}.demo_user_features",
                primary_keys=["user_id", "ts"], 
                timestamp_keys="ts", 
                df=user_features_df, 
                description="User Features")

destination_features_df = create_destination_features(spark.table(f"{catalog}.{schema}.demo_travel_purchase"))
fe.create_table(name=f"{catalog}.{schema}.demo_destination_features", 
                primary_keys=["destination_id", "ts"], 
                timestamp_keys="ts", 
                df=destination_features_df, 
                description="Destination Popularity Features")

destination_location = spark.table(f"{catalog}.{schema}.demo_destination_location")
fe.create_table(name=f"{catalog}.{schema}.demo_destination_location_features", 
                primary_keys="destination_id", 
                df=destination_location, 
                description="Destination location features.")

# COMMAND ----------

fe_get_table = fe.get_table(name=f"{catalog}.{schema}.demo_user_features")
print(f"Feature Table in UC = user_features. Description: {fe_get_table.description}")
print("The table contains those features: ", fe_get_table.features)

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, you can first **`create_table`** with a schema only, and populate data to the feature table with **`fs.write_table`**. To add data you can simply use **`fs.write_table`** again. **`fs.write_table`** supports a **`merge`** mode to update features based on the primary key. To overwrite a feature table you can simply `DELETE` the existing records directly from the feature table before writing new data to it, again with **`fs.write_table`**.
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```
# MAGIC fe.create_table(
# MAGIC     name="destination_location_fs",
# MAGIC     primary_keys=["destination_id"],
# MAGIC     schema=destination_features_df.schema,
# MAGIC     description="Destination Popularity Features",
# MAGIC )
# MAGIC
# MAGIC fe.write_table(
# MAGIC     name="destination_location_fs",
# MAGIC     df=destination_features_df
# MAGIC )
# MAGIC
# MAGIC # And then later/in the next run...
# MAGIC fe.write_table(
# MAGIC     name="destination_location_fs",
# MAGIC     df=updated_destination_features_df,
# MAGIC     mode="merge"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compute streaming features
# MAGIC
# MAGIC Availability of the destination can hugely affect the prices. Availability can change frequently especially around the holidays or long weekends during busy season. This data has a freshness requirement of every few minutes, so we use Spark structured streaming to ensure data is fresh when doing model prediction.

# COMMAND ----------

spark.sql('CREATE VOLUME IF NOT EXISTS demo_volume')
destination_availability_stream = (
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json") #Could be "kafka" to consume from a message queue
  .option("cloudFiles.inferSchema", "true")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .option("cloudFiles.schemaHints", "event_ts timestamp, booking_date date, destination_id int")
  .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/demo_volume/stream/availability_schema")
  .option("cloudFiles.maxFilesPerTrigger", 100) #Simulate streaming
  .load("/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json")
  .drop("_rescued_data")
  .withColumnRenamed("event_ts", "ts")
)

# COMMAND ----------

fe.create_table(
    name=f"{catalog}.{schema}.demo_availability_features", 
    primary_keys=["destination_id", "booking_date", "ts"],
    timestamp_keys=["ts"],
    schema=destination_availability_stream.schema,
    description="Destination Availability Features"
)

# Write the data to the feature table in "merge" mode using a stream
fe.write_table(
    name=f"{catalog}.{schema}.demo_availability_features", 
    df=destination_availability_stream,
    mode="merge",
    checkpoint_location= f"/Volumes/{catalog}/{schema}/demo_volume/stream/availability_checkpoint",
    # Refresh the feature store table once, or {'processingTime': '1 minute'} for every minute
    trigger={'once': True} 
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compute on-demand live features
# MAGIC
# MAGIC User location is a context feature that is captured at the time of the query, and not known in advance. Derivated features such as user distance from destination can only be computed at prediction time.
# MAGIC
# MAGIC With Feature Spec you can create a custom function (SQL/Python) to transform your data into new features, and link them to your model and feature store. The same code will be used for training and inference, whether batch or realtime.
# MAGIC
# MAGIC Note that this function will be available as `catalog.schema.distance_udf` in the browser.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION demo_distance_udf(lat1 DOUBLE, lon1 DOUBLE, lat2 DOUBLE, lon2 DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculate hearth distance from latitude and longitude'
# MAGIC AS $$
# MAGIC   import numpy as np
# MAGIC   dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
# MAGIC   a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
# MAGIC   return 2 * 6371 * np.arcsin(np.sqrt(a))
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Try it out
# MAGIC SELECT demo_distance_udf(user_latitude, user_longitude, latitude, longitude) AS hearth_distance, *
# MAGIC FROM demo_destination_location_features
# MAGIC JOIN demo_destination_features USING (destination_id)
# MAGIC JOIN demo_user_features USING (ts)
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train a model with batch, on-demand and streaming features

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get ground-truth training labels and key + timestamp

# COMMAND ----------

training_keys = spark.table(f"{catalog}.{schema}.demo_travel_purchase").select('ts', 'purchased', 'destination_id', 'user_id', 'user_latitude', 'user_longitude', 'booking_date')

# Split to define a training and inference set
training_df = training_keys.where("ts < '2022-11-23'")
test_df = training_keys.where("ts >= '2022-11-23'").cache()

display(training_df.limit(5))

# COMMAND ----------

feature_lookups = [ 
  FeatureLookup(
      table_name=f"{catalog}.{schema}.demo_user_features", 
      lookup_key="user_id",
      timestamp_lookup_key="ts",
      feature_names=["mean_price_7d"]
  ),
  FeatureLookup(
      table_name=f"{catalog}.{schema}.demo_destination_features", 
      lookup_key="destination_id",
      timestamp_lookup_key="ts"
  ),
  FeatureLookup(
      table_name=f"{catalog}.{schema}.demo_destination_location_features",  
      lookup_key="destination_id",
      feature_names=["latitude", "longitude"]
  ),
  FeatureLookup(
      table_name=f"{catalog}.{schema}.demo_availability_features", 
      lookup_key=["destination_id", "booking_date"],
      timestamp_lookup_key="ts",
      feature_names=["availability"]
  ),
  # TODO this requires USE CATALOG on system catalog
  # Add our function to compute the distance between the user and the destination 
#   FeatureFunction(
#       udf_name=f"{catalog}.{schema}.demo_distance_udf",
#       input_bindings={"lat1": "user_latitude", "lon1": "user_longitude", "lat2": "latitude", "lon2": "longitude"},
#       output_name="distance"
#   )
  ]

#Create the training set
training_set = fe.create_training_set(
    df=training_df,
    feature_lookups=feature_lookups,
    exclude_columns=['user_id', 'destination_id', 'booking_date', 'clicked', 'price'],
    label='purchased',
    use_spark_native_join=True # much faster with Photon
)
training_set_df = training_set.load_df()

# COMMAND ----------

display(training_set_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use AutoML to build an ML model out of the box

# COMMAND ----------

from databricks import automl

# Cache the training dataset for automl (to avoid recomputing it everytime)
training_features_df = training_set_df.cache()

summary_cl = automl.classify(
    dataset = training_features_df,
    target_col = "purchased",
    primary_metric="log_loss",
    timeout_minutes = 10
)

print(f"Best run id: {summary_cl.best_trial.mlflow_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save the best model to MLflow registry

# COMMAND ----------

model_name = "demo_fs_travel_model"
model_full_name = f"{catalog}.{schema}.{model_name}"

mlflow.set_registry_uri('databricks-uc')
# creating sample input to be logged (do not include the live features in the schema as they'll be computed within the model)
df_sample = training_set_df.limit(10).toPandas()
x_sample = df_sample.drop(columns=["purchased"])
dataset = mlflow.data.from_pandas(x_sample)

# getting the model created by AutoML 
best_model = summary_cl.best_trial.load_model()

#Get the conda env from automl run
artifacts_path = mlflow.artifacts.download_artifacts(run_id=summary_cl.best_trial.mlflow_run_id)
env = mlflow.pyfunc.get_default_conda_env()
with open(artifacts_path+"model/requirements.txt", 'r') as f:
    env['dependencies'][-1]['pip'] = f.read().split('\n')

#Create a new run in the same experiment as our automl run.
with mlflow.start_run(run_name="best_fs_model", experiment_id=summary_cl.experiment.experiment_id) as run:
  #Use the feature store client to log our best model
  mlflow.log_input(dataset, "training")
  fe.log_model(
              model=best_model, # object of your model
              artifact_path="model", #name of the Artifact under MlFlow
              flavor=mlflow.sklearn, # flavour of the model (our LightGBM model has a SkLearn Flavour)
              training_set=training_set, # training set you used to train your model with AutoML
              input_example=x_sample, # Dataset example (Pandas dataframe)
              registered_model_name=model_full_name, # register your best model
              conda_env=env)

  #Copy automl images & params to our FS run
  for item in os.listdir(artifacts_path):
    if item.endswith(".png"):
      mlflow.log_artifact(artifacts_path+item)
  mlflow.log_metrics(summary_cl.best_trial.metrics)
  mlflow.log_params(summary_cl.best_trial.params)
  mlflow.log_param("automl_run_id", summary_cl.best_trial.mlflow_run_id)
  mlflow.set_tag(key='feature_store', value='demo')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Move model to production

# COMMAND ----------

mlflow_client = MlflowClient()
# Use the MlflowClient to get a list of all versions for the registered model in Unity Catalog
all_versions = mlflow_client.search_model_versions(f"name='{model_full_name}'")
# Sort the list of versions by version number and get the latest version
latest_version = max([int(v.version) for v in all_versions])
# Use the MlflowClient to get the latest version of the registered model in Unity Catalog
latest_model = mlflow_client.get_model_version(model_full_name, str(latest_version))

# COMMAND ----------

production_alias = "production"
if len(latest_model.aliases) == 0 or latest_model.aliases[0] != production_alias:
  print(f"updating model {latest_model.version} to Production")
  mlflow_client = MlflowClient(registry_uri="databricks-uc")
  mlflow_client.set_registered_model_alias(model_full_name, production_alias, version=latest_model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run batch inference

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily leverage the feature store to get our predictions.
# MAGIC
# MAGIC No need to fetch or recompute the feature, we just need the lookup ids and the feature store will automatically fetch them from the feature store table. 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-flow-training.png?raw=true" width="1200px"/>

# COMMAND ----------

scored_df = fe.score_batch(model_uri=f"models:/{model_full_name}@{production_alias}", df=test_df, result_type="boolean")
display(scored_df)

# COMMAND ----------

from sklearn.metrics import accuracy_score

# simply convert the original probability predictions to true or false
pd_scoring = scored_df.select("purchased", "prediction").toPandas()
print("Accuracy: ", accuracy_score(pd_scoring["purchased"], pd_scoring["prediction"]))

# COMMAND ----------

# MAGIC %fs ls abfss://data@s00188devcussa2edap.dfs.core.windows.net/feature/

# COMMAND ----------

# MAGIC %fs ls abfss://data@s00188devcussa2edap.dfs.core.windows.net/feature/Store/StoreOperation

# COMMAND ----------

