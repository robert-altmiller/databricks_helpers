# Databricks notebook source
# DBTITLE 1,Environment Variables Initialization
# MAGIC  %run "./config"

# COMMAND ----------

# DBTITLE 1,Rest API Post Requests Functions
# get requests parameters
def get_params():
    params = {}
    return params


# get requests headers
def get_headers(token = None):
    headers = {'Authorization': 'Bearer %s' % token}
    return headers


# post request
def post_request(url = None, headers = None, params = None, data = None):
    if params != None:
        return requests.post(url, params = params, headers = headers, json = data)
    else: return requests.post(url, headers = headers, json = data)


# get request
def get_request(url = None, headers = None, params = None, data = None):
    if params != None:
        return requests.get(url, params = params, headers = headers, json = data)
    else: return requests.get(url, headers = headers, json = data)


# COMMAND ----------

# DBTITLE 1,Databricks Rest API 2.0 Configuration
def get_api_config(dbricks_instance = None, api_topic = None, api_call_type = None, dbricks_pat = None):
    config = {
        # databricks workspace instance
        "databricks_ws_instance": dbricks_instance,
        # databricks rest api version
        "api_version": "api/2.0",
        # databricks rest api service call
        "api_topic": api_topic,
        # databricks api call type
        "api_call_type": api_call_type
    }
    config["databricks_host"] = "https://" + config["databricks_ws_instance"]
    if api_topic != None and api_call_type != None:
      config["api_full_url"] = config["databricks_host"] + "/" + config["api_version"] + "/" + config["api_topic"] + "/" + config["api_call_type"]
    return config

# COMMAND ----------

# DBTITLE 1,Execute Databricks Rest API 2.0 Call (Generic)
# call_type variable is 'get' or 'post'
def execute_rest_api_call(function_call_type, config = None, token = None, jsondata = None):
    headers = get_headers(token)
    response = function_call_type(url = config["api_full_url"], headers = headers, data = jsondata)
    return response
