# Databricks notebook source
# DBTITLE 1,Create an MD5 Hash out of a Python String
def str_to_md5_hash(inputstr = None):
  """encode string using md5 hash"""
  return hashlib.md5(inputstr.encode())

# COMMAND ----------

# DBTITLE 1,Flatten Complex Nested Json
def format_json_col_as_struct(df = None, payload_col = None):
    """
    format spark json string column as a struct
    return type is a spark dataframe with json string columns formatted as a struct
    """
    json_schema = spark.read.json(df.rdd.map(lambda row: row[payload_col])).schema
    df = df.withColumn(payload_col, F.from_json(F.col(payload_col), json_schema))
    return df


def flatten_df(nested_df, prefix):
    """
    flatten nested json struct columns
    return type is spark dataframe with json struct column exploded into multiple spark columns
    """
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    flat_df = nested_df.select(flat_cols +
                                [F.col(nc + '.' + c).alias(prefix + c)
                                for nc in nested_cols
                                for c in nested_df.select(nc + '.*').columns])
    return flat_df

# COMMAND ----------

# DBTITLE 1,Create Databricks File System Folder
def create_dbfs_folder(folderpath = None):
  """create databricks file system folder"""
  try:
    result = dbutils.fs.mkdirs(folderpath)
    return f"{folderpath} created successfully...."
  except: return f"{folderpath} could not be created...."

# COMMAND ----------

# DBTITLE 1,Move File Into Databricks DBFS
def copy_file_into_dbfs(currentfilepath = None, dbfsnewfolderpath = None, overwrite = True):
  """move a local file in databricks into databricks file system"""
  try:
    result = create_dbfs_folder(dbfsnewfolderpath)
    currfolderpath = currentfilepath.rsplit("/", 1)[0]
    currfilename = currentfilepath.rsplit("/", 1)[1]
    dbfsnewfilepath = f"{dbfsnewfolderpath}/{currfilename}"
    dbutils.fs.cp(currentfilepath, dbfsnewfilepath, overwrite)
    return print(f"{currentfilepath} copied to {dbfsnewfilepath} successfully.....")
  except:
    return f"{currentfilepath} could not be copied to {dbfsnewfilepath}....."

# COMMAND ----------

# DBTITLE 1,Delete Databricks File System Folder
def delete_dbfs_folder(folderpath = None):
  """delete databricks file system folder"""
  try:
    result = dbutils.fs.rm(folderpath)
    return f"{folderpath} removed successfully...."
  except: return f"{folderpath} could not be removed...."

# COMMAND ----------

# DBTITLE 1,Get Databricks File System File Name
def get_dbfs_file_name(dbfsfilepath = None, file_ext = None):
  """get a dbfs file name"""
  files = dbutils.fs.ls(dbfsfilepath)
  for file in files:
    if file_ext in file[1]: return file[1]
  else: return None

# COMMAND ----------

# DBTITLE 1,Remove Invalid Characters From a String Input
def remove_invalid_chars(
        inputstr = None, 
        lowercase = False,
        uppercase = False,
        removenumbers = False,
        removespaces = False,
        removepunctuation = False,
        singledashes = False
    ):
    """remove all characters from python string besides letters dynamically"""
    if lowercase: inputstr = inputstr.lower()
    if uppercase: inputstr = inputstr.upper()
    if removenumbers: inputstr = re.sub(r'[0-9]', '', inputstr)
    if removespaces: inputstr = inputstr.replace(' ', '')
    if removepunctuation:
        punctuation = [punct for punct in str(string.punctuation)]
        punctuation.remove("-")
        for punct in punctuation:
            inputstr = inputstr.replace(punct, '')
    if singledashes: inputstr = re.sub(r'(-)+', r'-', inputstr)
    return inputstr


# COMMAND ----------

# DBTITLE 1,Remove a Substring From a String Input
def check_str_for_substr_and_replace(inputstr = None, substr = None):
    """remove a substring from a string input"""
    if substr in inputstr:
        return inputstr.replace(substr, '')
    else: return inputstr

# COMMAND ----------

# DBTITLE 1,Url Encoding HTML String Function
def url_encode_str(inputstr = None):
  """url encode an input string"""
  return urllib.parse.quote_plus(inputstr)

# COMMAND ----------

# DBTITLE 1,Remove Null and Blank Attributes from Complex Nested Json Dictionary
def remove_blank_attributes(json_dict):
  """
  recursive function to remove all null, none, [], and other blank
  attributes from a complex nested json dictionary
  """
  cleaned_dict = {}
  for key, value in json_dict.items():
    if value:
      if isinstance(value, dict):
        cleaned_value = remove_blank_attributes(value)
        if cleaned_value: cleaned_dict[key] = cleaned_value
      elif isinstance(value, list):
        cleaned_list = []
        for item in value:
          if isinstance(item, dict):
            cleaned_item = remove_blank_attributes(item)
            if cleaned_item: cleaned_list.append(cleaned_item)
          elif item: cleaned_list.append(item)
        if cleaned_list: cleaned_dict[key] = cleaned_list
      else: cleaned_dict[key] = value
  return cleaned_dict

# COMMAND ----------

# DBTITLE 1,Upload Databricks API Results to Azure Storage and DBFS
def upload_to_dbfs_and_azure_storage(azstorageobj, instructions):
  # write dbricks api results to DBFS
  schema = StructType(
    [
      StructField('payload', StringType(), True)
    ]
  )
  df = spark.createDataFrame(data = [[instructions]], schema = schema)


  dbfsfilepath = f'{azstorageobj.config["AZURE_STORAGE_ACCOUNT_FOLDER_PATH"]}/{azstorageobj.config["AZURE_STORAGE_ACCOUNT_SUBFOLDER_PATH"]}/{azstorageobj.config["AZURE_STORAGE_ACCOUNT_FILE_NAME"]}'
  # clean up old dbfs dbricks api results
  dbutils.fs.rm(dbfsfilepath, True)
  df.coalesce(1).write.mode("overwrite").format('json').save(dbfsfilepath)


  # write dbricks api results to azure storage account
  processing = True
  while(processing):
      try:
        azstorageobj.delete_container(azstorageobj.config["AZURE_STORAGE_ACCOUNT_CONTAINER"])
        processing = False
      except:
        time.sleep(5) # need time for azure storage account container to be deleted
  azstorageobj.upload_blob_from_local(
    storageacctname = azstorageobj.config["AZURE_STORAGE_ACCOUNT_NAME"], 
    container = azstorageobj.config["AZURE_STORAGE_ACCOUNT_CONTAINER"],
    localfilepath = f'/dbfs/{dbfsfilepath}/{get_dbfs_file_name(dbfsfilepath, ".json")}', 
    blobfilepath = dbfsfilepath, 
    overwrite = True
  )

  # finally remove new dbfs dbricks api results
  dbutils.fs.rm(dbfsfilepath, True)
