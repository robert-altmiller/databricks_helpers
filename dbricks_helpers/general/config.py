# Databricks notebook source
# DBTITLE 1,Import Libraries
# MAGIC %run "./libraries"

# COMMAND ----------

# DBTITLE 1,Import Generic Functions
# MAGIC %run "./general_functions"

# COMMAND ----------

# DBTITLE 1,Import Azure Functions
# MAGIC %run "../azure/main"

# COMMAND ----------

# DBTITLE 1,Configuration Class Initialization
# load .env environment file
load_dotenv()

class Config:
# configuration class definition

    # class constructor    
    def __init__(self):

        # variables
        self.ENVIRONMENT = str(os.getenv('ENVIRONMENT'))
        self.LOCAL_DATA_FOLDER = str(os.getenv('LOCAL_DATA_FOLDER'))
        self.DATABRICKS_INSTANCE = str(spark.conf.get("spark.databricks.workspaceUrl"))
        self.DATABRICKS_PAT = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
        self.DATABRICKS_MIGRATION_INSTANCE = str(os.getenv('DATABRICKS_MIGRATION_INSTANCE'))
        self.DATABRICKS_MIGRATION_PAT = str(os.getenv('DATABRICKS_MIGRATION_PAT'))
        self.AZURE_STORAGE_ACCOUNT_NAME = f"{str(os.getenv('AZURE_STORAGE_ACCOUNT_NAME'))}"
        self.AZURE_STORAGE_ACCOUNT_CONTAINER = str(os.getenv('AZURE_STORAGE_ACCOUNT_CONTAINER'))
        self.AZURE_STORAGE_ACCOUNT_SUBFOLDER_PATH = str('')
        self.AZURE_STORAGE_ACCOUNT_FOLDER_PATH = f'databricks/{self.DATABRICKS_INSTANCE}'
        self.AZURE_STORAGE_ACCOUNT_FILE_NAME = str(os.getenv('AZURE_STORAGE_ACCOUNT_FILE_NAME'))
        self.AZURE_STORAGE_ACCOUNT_KEY = str(os.getenv('AZURE_STORAGE_ACCOUNT_KEY'))
        self.AZURE_STORAGE_ACCOUNT_CONN = f'DefaultEndpointsProtocol=https;AccountName={self.AZURE_STORAGE_ACCOUNT_NAME};AccountKey={self.AZURE_STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net'
        self.AZURE_STORAGE_ACCOUNT_SAS_TOKEN = str('')
        self.format_config_vars()
        

    def format_config_vars(self):
        """
        additional formatting for configuration variables
        this function is optional if you need it
        """
        return None


    def get_config_vars(self):
        # get class configuration variables
        config = Config()
        return vars(config)


    def print_config_vars(self):
        # get configuration variables in a python dictionary
        variables = self.get_config_vars()
        print("configuration variables:")
        vars_list = []
        for key, val in variables.items():
            print(f"{key}: {val}")
        print("\n")

# COMMAND ----------

# DBTITLE 1,Variables Initialization

# configuration class object
config = Config()
# print configuration variables
config.print_config_vars()


# get configuration variables
config = config.get_config_vars()


# databricks instance address
databricks_instance = config["DATABRICKS_INSTANCE"]
print(f"databricks_instance: {databricks_instance}")
# databricks personal access token
databricks_pat = config["DATABRICKS_PAT"]
print(f"databricks_pat: {databricks_pat}")
# databricks migration instance address
databricks_migration_instance = config["DATABRICKS_MIGRATION_INSTANCE"]
print(f"databricks_migration_instance: {databricks_migration_instance}")
# databricks migration personal access token
databricks_migration_pat = config["DATABRICKS_MIGRATION_PAT"]
print(f"databricks_migration_pat: {databricks_migration_pat}")

# COMMAND ----------

# DBTITLE 1,Class Objects Initialization
# az storage account class object
storage_account_obj = azurestorageaccount(config)
