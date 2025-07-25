# Databricks notebook source
# DBTITLE 1,Pip Install Libraries
# MAGIC %pip install --upgrade openai fastapi mlflow dotenv

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
import os
from openai import AzureOpenAI
from openai import OpenAI
from fastapi import FastAPI, Request
import pandas as pd
from pydantic import BaseModel
from dotenv import load_dotenv

import mlflow
import mlflow.pyfunc
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

mlflow.set_registry_uri('databricks-uc')  # Set the MLflow registry URI to Databricks Unity Catalog
mlflow.openai.autolog()

# COMMAND ----------

# DBTITLE 1,MLFLOW Helper Functions
def get_max_model_version(uc_model_name: str) -> int:
    """get the max model version for a unity catalog model"""
    model_versions_list = []
    model_versions = mlflow.search_model_versions(filter_string=f"name='{uc_model_name}'")
    for version in model_versions: 
        model_versions_list.append(int(version.version))
    return max(model_versions_list)

def get_model_path(uc_model_name: str) -> str:
    """get the unity catalog model path"""
    return f"models:/{uc_model_name}/{get_max_model_version(uc_model_name)}"

def load_model(uc_model_name: str) -> mlflow.pyfunc.load_model:
    """load unity catalog model"""
    return mlflow.pyfunc.load_model(get_model_path(uc_model_name))

def register_model_in_uc(uc_model_name: str, log_model: mlflow.models.model.ModelInfo) -> mlflow.entities.model_registry.model_version.ModelVersion:
    """register model in unity catalog and get the registered model info"""
    return mlflow.register_model(model_uri=log_model.model_uri, name=uc_model_name)

# COMMAND ----------

# DBTITLE 1,Local Parameters
catalog_name = "boeingeastus"
schema_name = "ml-test"
uc_model_name = f"{catalog_name}.{schema_name}.llm_router_model"  # Define the model name using the catalog name

# Databricks PAT token
pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Databricks foundational model parameters
workspace_url = "https://adb-4247081124391553.13.azuredatabricks.net"
databricks_model_serving_ep_url = f"{workspace_url}/api/2.0/serving-endpoints"
foundation_model_ep = f"{workspace_url}/serving-endpoints"
foundation_model_name = "databricks-claude-3-7-sonnet"

# OpenAI foundation model parameters
openai_endpoint = os.getenv("ENDPOINT_URL", "https://appvec.openai.azure.com/")
openai_deployment_name = os.getenv("DEPLOYMENT_NAME", "alt-o3")
openai_subscription_key = os.getenv("AZURE_OPENAI_API_KEY", "XXXXXXXX")
openai_api_version = "2024-12-01-preview"

# MLFLOW parameters
model_serving_ep_name = "llm_router_inference_ep"

# COMMAND ----------

# DBTITLE 1,Unit Test Using Databricks Foundation Model
client = OpenAI(
    api_key=pat_token,
    base_url=foundation_model_ep
)

response = client.chat.completions.create(
    model=foundation_model_name,
    messages=[
        {
            "role": "user",
            "content": "What is an LLM agent?"
        }
    ]
)

print(response.choices[0].message.content)

# COMMAND ----------

# DBTITLE 1,LLM Router Model Class
# class LLMRouterModel(mlflow.pyfunc.PythonModel):
#     def __init__(self, pat_token: str, openai_deployment_name: str, foundation_model_ep: str, foundation_model_name: str):
#         # Capture your deployment so it's pickled into the model.
#         self.foundation_model_ep = foundation_model_ep
#         self.foundation_model_name = foundation_model_name
#         self.openai_deployment_name = openai_deployment_name
#         self.pat_token = pat_token

#     def load_context(self, context):
#         with open(context.artifacts["openai_api_key"]) as key:
#             api_key = key.read()
#         with open(context.artifacts["openai_api_version"]) as version:
#             api_version = version.read()
#         with open(context.artifacts["openai_endpoint"]) as ep:
#             api_endpoint = ep.read()
#         self.client = AzureOpenAI(
#             api_version=api_version,
#             api_key = api_key,
#             azure_endpoint = api_endpoint
#         )
    
#     def predict_openai(self, context, model_input):
#         prompt = model_input["prompt"][0]
#         response = self.client.chat.completions.create(
#             model=self.openai_deployment_name,
#             messages=[{"role": "user", "content": prompt}],
#             max_completion_tokens=10000
#         )
#         return [response.choices[0].message.content]

#     def predict_claude(self, context, model_input):
#         prompt = model_input["prompt"][0]
#         client = OpenAI(
#             api_key=self.pat_token,
#             base_url=self.foundation_model_ep
#         )
#         response = client.chat.completions.create(
#             model=self.foundation_model_name,
#             messages=[{"role": "user", "content": prompt}]
#         )
#         return [response.choices[0].message.content]

#     def predict(self, context, model_input):
#         model_type = model_input["model_type"][0]
#         if model_type == "openai":
#             return self.predict_openai(context, model_input)
#         if model_type == "claude":
#             return self.predict_claude(context, model_input)
#         else: return self.predict_openai(context, model_input)

# Unit Test
# m = LLMRouterModel(pat_token, openai_deployment_name, foundation_model_ep, foundation_model_name)
# # fake context object
# class C: artifacts = {"openai_api_key": "openai_api_key.txt", "openai_api_version": "openai_api_version.txt", "openai_endpoint": "openai_endpoint.txt"}
# m.load_context(C)
# print(m.predict(None, {"model_type": ["openai"], "prompt":["what is your name"]}))
# print(m.predict(None, {"model_type": ["claude"], "prompt":["what is your name"]}))

# COMMAND ----------

# DBTITLE 1,Create MLFLOW Experiment
# Save the api_key and endpoint to files
with open("openai_api_key.txt", "w") as f:
    f.write(openai_subscription_key.strip())
with open("openai_api_version.txt", "w") as f:
    f.write(openai_api_version.strip())
with open("openai_endpoint.txt", "w") as f:
    f.write(openai_endpoint.strip())

# Create model signature
input_schema = Schema([ColSpec("string", "model_type"), ColSpec("string", "prompt")])
output_schema = Schema([ColSpec("string")])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

with mlflow.start_run() as run:
     log_model = mlflow.pyfunc.log_model(
        artifact_path="llm_router_model",
        python_model="llm_router_model.py",
        artifacts={
            "openai_api_key": "openai_api_key.txt",
            "openai_api_version": "openai_api_version.txt",
            "openai_endpoint": "openai_endpoint.txt"
        },
        signature=signature,
        code_paths=["llm_router_model.py"],
        conda_env="environment.yaml"
    )
run_id = run.info.run_id

# Register the model in Unity Catalog and load it
uc_registered_model_info = register_model_in_uc(uc_model_name, log_model)

# COMMAND ----------

# DBTITLE 1,Test Prediction Using Model
import ast

model = load_model(uc_model_name)

# Use the model for prediction
model_input = [{"model_type": "openai", "prompt": "what is a databricks lakehouse?"}]
result = model.predict(model_input)

# refine results using different model
instructions = """
Read the text below and extract its top 3 major points.  
Return **only** a valid Python list literal of three strings, formatted exactly like: ["First full sentence.", "Second full sentence.", "Third full sentence."]
Requirements:
1. Each element must be a complete sentence.
2. Do **not** wrap the entire list in quotes or code fences.
3. Do **not** include backslashes (\\), newline escapes (\n), or any extra characters.
4. Your response must start with `["` and end with `"]` only.
"""
model_input = [{"model_type": "claude", "prompt": f"{instructions}\n\n{result}"}]
results = ast.literal_eval(list(model.predict(model_input))[0])
for result in results: print(result)

# COMMAND ----------

# DBTITLE 1,Serve the Model
import requests

response = requests.post(
    url=databricks_model_serving_ep_url,
    headers={
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json"
    },
    json={
        "name": model_serving_ep_name,
        "config": {
            "served_models": [
                {
                    "model_name": uc_model_name,
                    "model_version": uc_registered_model_info.version, 
                    "workload_type": "CPU",
                    "workload_size": "Small",  # <-- REQUIRED FIELD
                    "scale_to_zero_enabled": True
                }
            ]
        }
    }
)

print("Status Code:", response.status_code)
print("Response:", response.text)

# COMMAND ----------

# DBTITLE 1,Test Model Serving Inference Endpoint

# Model serving inference endpoint (e.g., invocations)
url = f"{workspace_url}/serving-endpoints/{model_serving_ep_name}/invocations"
print(f"url: {url}")
headers = {
    "Authorization": f"Bearer {pat_token}",
    "Content-Type": "application/json"
}

# Match the structure expected by your model's `predict()` method
data = {
    "dataframe_records": [
        {"prompt": "What is the difference between a data lake and a lakehouse?"}
    ]
}

response = requests.post(url, headers=headers, json=data)

# Handle response
print("Status:", response.status_code)
try:
    print(response.json())
except Exception:
    print(response.text)

# COMMAND ----------

