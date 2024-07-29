# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Demo Access Data With SAS Tokens

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 'altmiller-users' local workspace group has user 'robert.altmiller@databrick.com'<br>
# MAGIC - ##### has access to the SAS tokens which point to the 'dbfs-files' container.
# MAGIC ## 'altmiller-contributors' local workspace group has user 'chandra.peddireddy@databricks.com'<br>
# MAGIC - ##### has access to the SAS tokens which point to the 'dbfs-files-nav' container.

# COMMAND ----------

# DBTITLE 1,Get Secret Scope Base Functions and Libraries
# MAGIC %run "../general/base"

# COMMAND ----------

# DBTITLE 1,Supernal Variable Declarations
method = "adlsgen2" # or adlsgen2


#-------------------------------------NON ADLS GEN 2 METHOD-----------------------------------


if method == "storageaccount":
    
    # storage account name
    storage_account_name = "altstoragedev"
    # ft = flight tech secret scope variables
    scope_name_ft = "flight_tech_secretscope"
    secret_name_ft_lf = "flight-tech-sas-token-dbfsfiles" # list files
    secret_name_ft_rf_pdf = "flight-tech-sas-token-dbfsfiles" # read files

    scope_name_autnav = "autonomy_nav_secretscope"
    secret_name_autnav_lf = "autonomy-nav-sas-token-dbfsfilesnav" # list files
    secret_name_autnav_rf_matlab = "autonomy-nav-sas-token-dbfsfilesnav" # read files
    secret_name_autnav_rf_imgs = "autonomy-nav-sas-token-dbfsfilesnav" # read files

#-------------------------------------ADLS GEN 2 METHOD -------------------------------------


if method == "adlsgen2":

    # storage account name
    storage_account_name = "altstorageadlsgen2"
    # ft = flight tech secret scope variables
    scope_name_ft = "flight_tech_secretscope"
    secret_name_ft_lf = "flight-tech-sas-token-dbfsfiles-listfiles" # list files
    secret_name_ft_rf_pdf = "autonomy-nav-sas-token-dbfsfilesnav-readfiles-pdfs" # read files

    # autnav = autonomy / navigation secret scope variables
    scope_name_autnav = "autonomy_nav_secretscope"
    secret_name_autnav_lf = "autonomy-nav-sas-token-dbfsfilesnav-listfiles" # list files
    secret_name_autnav_rf_matlab = "autonomy-nav-sas-token-dbfsfilesnav-readfiles-matlab" # read files
    secret_name_autnav_rf_imgs = "autonomy-nav-sas-token-dbfsfilesnav-readfiles-imgs" # read files


# container 'dbfs-files' settings
az_container_ft = "dbfs-files"
az_storage_acct_fldr_path_pdfs_ft = "pdfs"
#az_storage_acct_fldr_path_pdfs_ft = "morefiles/pdfs"
base_sas_url_pdfs_ft = f"https://{storage_account_name}.blob.core.windows.net/{az_container_ft}/{az_storage_acct_fldr_path_pdfs_ft}" # use with sas token

#container 'dbfs-files-nav' settings
az_container_autnav = "dbfs-files-nav"
az_fldr_path_images_autnav = "images"
az_fldr_path_matlab_autnav = "matlab"
base_sas_url_images_autnav = f"https://{storage_account_name}.blob.core.windows.net/{az_container_autnav}/{az_fldr_path_images_autnav}" # use with sas token
base_sas_url_matlab_autnav = f"https://{storage_account_name}.blob.core.windows.net/{az_container_autnav}/{az_fldr_path_matlab_autnav}" # use with sas token

# set the storage account name override
storage_account_obj.set_azure_storage_acct_name_override(storage_account_name)

# COMMAND ----------

# DBTITLE 1,Read 'Flight Tech' SAS Token Secret Scope Using DBUTILS
try:
    sas_token_ft_lf = dbutils.secrets.get(scope = scope_name_ft, key = secret_name_ft_lf)
    print(f"secret: '{secret_name_ft_lf}' and secret_val: '{sas_token_ft_lf.replace('', ' ')}'")
except: print(f"unable to get secret value for list sas token for dbfs-files container....")
try:
    sas_token_ft_rf_pdf = dbutils.secrets.get(scope = scope_name_ft, key = secret_name_ft_rf_pdf)
    print(f"secret: '{secret_name_ft_rf_pdf}' and secret_val: '{sas_token_ft_rf_pdf.replace('', ' ')}'")
except: print(f"unable to get secret value for read sas token for pdf data folder....")

# COMMAND ----------

# DBTITLE 1,Read 'Autonomy and Navigation' SAS Token Secret Scope Using DBUTILS
try:
    sas_token_autnav_lf = dbutils.secrets.get(scope = scope_name_autnav, key = secret_name_autnav_lf)
    print(f"secret: '{secret_name_autnav_lf}' and secret_val: '{sas_token_autnav_lf.replace('', ' ')}'")
except: print(f"unable to get secret value for list sas token for dbfs-files-nav container....")

try:
    sas_token_autnav_rf_matlab = dbutils.secrets.get(scope = scope_name_autnav, key = secret_name_autnav_rf_matlab)
    print(f"secret: '{secret_name_autnav_rf_matlab}' and secret_val: '{sas_token_autnav_rf_matlab.replace('', ' ')}'")
except: print(f"unable to get secret value for read sas token for matlab data folder....")

try:
    sas_token_autnav_rf_imgs = dbutils.secrets.get(scope = scope_name_autnav, key = secret_name_autnav_rf_imgs)
    print(f"secret: '{secret_name_autnav_rf_imgs}' and secret_val: '{sas_token_autnav_rf_imgs.replace('', ' ')}'")
except: print(f"unable to get secret value for read sas token images data folder....")

# COMMAND ----------

# DBTITLE 1,Download Blob Files Using Storage Account Key or SAS Token
def download_blob_files(blobfileslist = None, storaceaccount = None, container = None, sas_base_url = None, sas_token = None):
    """iterate and download all files in blob folder"""
    for file in blobfileslist:
        foldername = file.split("/", 1)[0]
        filename = file.rsplit("/", 1)[1]
        subfoldername = file.split(foldername)[1].split(filename)[0].replace('/', '')
        if sas_base_url != None and sas_token != None:
            blob_url = f"{sas_base_url}/{filename}{sas_token}"
            response = requests.get(blob_url)
            data_by_sas = response.content
            storage_account_obj.download_blob_write_locally(storaceaccount, container, foldername, subfoldername, filename, data_by_sas)
        else: storage_account_obj.download_blob_write_locally(storaceaccount, container, foldername, subfoldername, filename)
        print(f"downloaded '{file.split('/')[1]}' to local repo 'data' folder\n")

# COMMAND ----------

# DBTITLE 1,Read Flight Tech Data From Azure Storage Account Container
try:# get flight tech pdf file paths
    fileslistpdf = storage_account_obj.listblobfiles(
        storageacctname = storage_account_name, 
        container = az_container_ft, 
        folderpath = f"{az_storage_acct_fldr_path_pdfs_ft}/", 
        sas_token = sas_token_ft_lf
    )
    download_blob_files(fileslistpdf, storage_account_name, az_container_ft, base_sas_url_pdfs_ft, sas_token_ft_rf_pdf) # uses sas token to get data
except: print("user does not have access....")

# COMMAND ----------

# DBTITLE 1,Read Autonomy and Navigation Data From Azure Storage Account Container
try:
    # get autonomy and navigation image file paths
    fileslistimg = storage_account_obj.listblobfiles(
        storageacctname = storage_account_name,
        container = az_container_autnav,
        folderpath = f"{az_fldr_path_images_autnav}/",
        sas_token = sas_token_autnav_lf
    )
    download_blob_files(fileslistimg, storage_account_name, az_container_autnav, base_sas_url_images_autnav, sas_token_autnav_rf_imgs) # uses sas token to get data
except: print("user does not have access....")

try:
    # get autonomy and navigation matlab file paths
    filelistmat = storage_account_obj.listblobfiles(
        storageacctname = storage_account_name, 
        container = az_container_autnav, 
        folderpath = f"{az_fldr_path_matlab_autnav}/", 
        sas_token = sas_token_autnav_lf
    )
    download_blob_files(filelistmat, storage_account_name, az_container_autnav, base_sas_url_matlab_autnav, sas_token_autnav_rf_matlab) # uses sas token to get data
except: print("user does not have access....")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ADDITIONAL CODE BELOW FOR CREATING AN AZURE BACKED DATABRICKS SECRET SCOPE

# COMMAND ----------

# DBTITLE 1,EXAMPLE: Create an Azure Backed Databricks Secret Scope
# import requests
# import json

# # define the endpoint URL
# url = "https://<databricks-instance>/api/2.0/secrets/scopes/create"

# # replace <databricks-instance> with your Databricks workspace URL
# # replace <personal-access-token> with your Databricks personal access token

# headers = {
#     "Authorization": "Bearer <personal-access-token>"
# }

# data = {
#   "scope": "<scope-name>",  # name of the secret scope
#   "initial_manage_principal": "users",  # principal that can manage the secret scope
#   "backend_type": "AZURE_KEYVAULT",  # backend type
#   "backend_azure_keyvault": {
#     "resource_id": "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.KeyVault/vaults/<keyvault-name>",  # replace with Azure KeyVault resource ID
#     "dns_name": "<keyvault-dns-name>"  # replace with Azure KeyVault DNS name
#   }
# }

# response = requests.post(url, headers=headers, json=data)

# # print the response from the POST request
# print(response.json())

# COMMAND ----------

# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# blobs = BlobServiceClient(f"https://{storage_account_name}.blob.core.windows.net/", credential = sas_token_ft).get_container_client(az_container_autnav).list_blobs()
# for blob in blobs:
#     print(blob)
