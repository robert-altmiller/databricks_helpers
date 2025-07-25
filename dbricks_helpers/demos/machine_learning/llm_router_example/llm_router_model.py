import os
import mlflow
from mlflow.models import set_model
from mlflow.pyfunc import PythonModel
from openai import OpenAI, AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

# Retrieve environment variables
pat_token = os.getenv("PAT_TOKEN")
openai_deployment_name = os.getenv("OPENAI_DEPLOYMENT_NAME")
foundation_model_ep = os.getenv("FOUNDATION_MODEL_EP")
foundation_model_name = os.getenv("FOUNDATION_MODEL_NAME")

# LLM Router Class
class LLMRouterModel(mlflow.pyfunc.PythonModel):
    def __init__(self, pat_token: str, openai_deployment_name: str, foundation_model_ep: str, foundation_model_name: str):
        # Capture your deployment so it's pickled into the model.
        self.foundation_model_ep = foundation_model_ep
        self.foundation_model_name = foundation_model_name
        self.openai_deployment_name = openai_deployment_name
        self.pat_token = pat_token

    def load_context(self, context):
        with open(context.artifacts["openai_api_key"]) as key:
            api_key = key.read()
        with open(context.artifacts["openai_api_version"]) as version:
            api_version = version.read()
        with open(context.artifacts["openai_endpoint"]) as ep:
            api_endpoint = ep.read()
        self.client = AzureOpenAI(
            api_version=api_version,
            api_key = api_key,
            azure_endpoint = api_endpoint
        )
    
    def predict_openai(self, context, model_input):
        prompt = model_input["prompt"][0]
        response = self.client.chat.completions.create(
            model=self.openai_deployment_name,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=10000
        )
        return [response.choices[0].message.content]

    def predict_claude(self, context, model_input):
        prompt = model_input["prompt"][0]
        client = OpenAI(
            api_key=self.pat_token,
            base_url=self.foundation_model_ep
        )
        response = client.chat.completions.create(
            model=self.foundation_model_name,
            messages=[{"role": "user", "content": prompt}]
        )
        return [response.choices[0].message.content]

    def predict(self, context, model_input):
        model_type = model_input["model_type"][0]
        if model_type == "openai":
            return self.predict_openai(context, model_input)
        if model_type == "claude":
            return self.predict_claude(context, model_input)
        else: return self.predict_openai(context, model_input)

set_model(LLMRouterModel(pat_token, openai_deployment_name, foundation_model_ep, foundation_model_name))