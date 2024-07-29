# Databricks notebook source
# DBTITLE 1,Get Requirements File Path
import os
requirementspath = os.getcwd() + "/requirements.txt"
# path will change depending on what notebook this is called from
requirementspath = requirementspath.rsplit("/", 2)[0] + "/general/requirements.txt"
os.environ["requirementspath"] = requirementspath
print(requirementspath)

# COMMAND ----------

# DBTITLE 1,Install Library Requirements
# MAGIC %sh
# MAGIC
# MAGIC # local train data filepath
# MAGIC for path in $requirementspath 
# MAGIC   do 
# MAGIC     requirementspath="$path" 
# MAGIC   done
# MAGIC echo $requirementspath
# MAGIC
# MAGIC pip install -r $requirementspath

# COMMAND ----------

# DBTITLE 1,Library Imports
# library and file imports
import json, time, requests, hashlib, string, random, pathlib, re, shutil, urllib.parse
from datetime import datetime

# numpy and pandas
import pandas as pd
import numpy as np

# dot env for environment variables in .env file
from dotenv import load_dotenv

# pyspark imports
import pyspark.sql.functions as F
from pyspark.sql.types import *

