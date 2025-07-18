# Databricks notebook source
# MAGIC %pip install llama-index llama-index-llms-azure-openai llama-index-embeddings-azure-openai

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports
import os
from llama_index.core import (
    VectorStoreIndex,
    SimpleDirectoryReader,
    StorageContext,
    load_index_from_storage,
    Settings,
)
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding

# COMMAND ----------

# DBTITLE 1,Library Imports and Environment Variables
# Azure OpenAI deployment parameter
os.environ['OPENAI_ENDPOINT_NAME'] = "https://alt-open-ai.openai.azure.com"
os.environ['OPENAI_API_KEY'] = "XXXXXXXXXX"
os.environ['OPENAI_API_VERSION'] = "2025-01-01-preview"
os.environ["OPENAI_API_MODEL"] = "o3"
os.environ['OPENAI_API_DEPLOYMENT_NAME'] = "alt-o3"

# Azure OpenAI embedding model parameters
os.environ["OPENAI_EMBEDDING_MODEL"] = "text-embedding-ada-002"
os.environ["OPENAI_EMBEDDING_MODEL_API_VERSION"] = "2024-12-01-preview" 
os.environ["OPENAI_EMBEDDING_MODEL_DEPLOYMENT"] = "alt-text-embedding-ada-002"

# COMMAND ----------

# DBTITLE 1,Read Environment Variables
# Azure OpenAI deployment parameter
AZURE_OPENAI_ENDPOINT = os.getenv("OPENAI_ENDPOINT_NAME")
AZURE_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AZURE_OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
AZURE_OPENAI_API_MODEL = os.getenv("OPENAI_API_MODEL")
AZURE_OPENAI_DEPLOYMENT = os.getenv("OPENAI_API_DEPLOYMENT_NAME")

# Azure OpenAI embedding model parameters
AZURE_OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL")
AZURE_OPENAI_EMBEDDING_MODEL_API_VERSION = os.getenv("OPENAI_EMBEDDING_MODEL_API_VERSION")
AZURE_OPENAI_EMBEDDING_MODEL_DEPLOYMENT = os.getenv("OPENAI_EMBEDDING_MODEL_DEPLOYMENT")

# COMMAND ----------

# DBTITLE 1,Test OpenAI LLM Deployment Endpoint
import os
import base64
from openai import AzureOpenAI


# Initialize Azure OpenAI client with key-based authentication
client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
)


#Prepare the chat prompt
chat_prompt = []

# Include speech result if speech is enabled
messages = [{"role": "user", "content": "What a plane flight?"}]

# Generate the completion
completion = client.chat.completions.create(
    model=AZURE_OPENAI_DEPLOYMENT,
    messages=messages,
    max_completion_tokens=100000,
    temperature=1,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None,
    stream=False
)

print(completion.to_json())

# COMMAND ----------

# Adapted from: https://docs.llamaindex.ai/en/stable/examples/customization/llms/AzureOpenAI/

# === Initialize LLM and Embedding Models ===

llm = AzureOpenAI(
    model="o3",  # Logical model name
    deployment_name=AZURE_OPENAI_DEPLOYMENT,  # Azure deployment name
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION,
)

embed_model = AzureOpenAIEmbedding(
    model=AZURE_OPENAI_EMBEDDING_MODEL,
    deployment_name=AZURE_OPENAI_EMBEDDING_MODEL_DEPLOYMENT,
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_EMBEDDING_MODEL_API_VERSION,
)

# Set global LLM and embedding settings
Settings.llm = llm
Settings.embed_model = embed_model

# === Index Persistence Setup ===

PERSIST_DIR = "./storage"

if not os.path.exists(PERSIST_DIR):
    # Load and index documents on first run
    documents = SimpleDirectoryReader('./data').load_data()
    index = VectorStoreIndex.from_documents(documents)
    index.storage_context.persist(persist_dir=PERSIST_DIR)
else:
    # Load existing index from disk
    storage_context = StorageContext.from_defaults(persist_dir=PERSIST_DIR)
    index = load_index_from_storage(storage_context)

# === Query the Index ===

query_engine = index.as_query_engine()
response = query_engine.query("What is the first article about?")
print(response)