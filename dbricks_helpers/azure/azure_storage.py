# Databricks notebook source
# DBTITLE 1,Azure Storage Class
# library imports
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


# azure storage account class functions
class azurestorageaccount(azureclass):
    
    # class constructor    
    def __init__(self, config):
        # get all configuration variables
        self.filename = None
        super().__init__(config)
    

    def set_azure_storage_acct_name_override(self, az_storage_account_name = None):
        """set a new azure storage account name"""
        self.config["AZURE_STORAGE_ACCOUNT_NAME"] = az_storage_account_name


    def set_azure_storage_acct_container_name_override(self, az_storage_account_container_name = None):
        """set a new azure storage account container name"""
        self.config["AZURE_STORAGE_ACCOUNT_CONTAINER"] = az_storage_account_container_name


    def set_azure_storage_acct_sas_token_override(self, azure_storage_acct_sas_token = None):
        """set a new azure client id (e.g. service principle password)"""
        self.config["AZURE_STORAGE_ACCOUNT_SAS_TOKEN"] = azure_storage_acct_sas_token


    def set_azure_storage_acct_folder_path_override(self, az_storage_acct_folderpath = None):
        """set a new azure storage account folder path"""
        self.config["AZURE_STORAGE_ACCOUNT_FOLDER_PATH"] = az_storage_acct_folderpath


    def set_azure_storage_acct_subfolder_path_override(self, az_storage_acct_subfolderpath = None):
        """set a new azure storage account folder path"""
        self.config["AZURE_STORAGE_ACCOUNT_SUBFOLDER_PATH"] = az_storage_acct_subfolderpath


    def set_azure_storage_acct_file_name_override(self, az_storage_acct_filename = None):
        """set a new azure storage account file name"""
        self.config["AZURE_STORAGE_ACCOUNT_FILE_NAME"] = az_storage_acct_filename

    
    def create_blob_service_client(self):
        """create azure storage blob service client"""
        return BlobServiceClient.from_connection_string(self.config["AZURE_STORAGE_ACCOUNT_CONN"])


    def create_blob_service_client_sas(self):
        """create azure storage blob service client using shared access signature token"""
        return BlobServiceClient(f"https://{self.config['AZURE_STORAGE_ACCOUNT_NAME']}.blob.core.windows.net/", credential = self.config["AZURE_STORAGE_ACCOUNT_SAS_TOKEN"])


    def create_container_client(self, sas_token = None):
        """create azure storage account container client"""
        if sas_token != None: # use sas token for auth
            storage_account_obj.set_azure_storage_acct_sas_token_override(sas_token)
            return self.create_blob_service_client_sas().get_container_client(self.config["AZURE_STORAGE_ACCOUNT_CONTAINER"])
        else: return self.create_blob_service_client().get_container_client(self.config["AZURE_STORAGE_ACCOUNT_CONTAINER"])


    def get_blob_file_path(self):
        """get blob file path for download from azure storage account container"""
        blobfilepath = f'{self.config["AZURE_STORAGE_ACCOUNT_FOLDER_PATH"]}/{self.config["AZURE_STORAGE_ACCOUNT_SUBFOLDER_PATH"]}/{self.config["AZURE_STORAGE_ACCOUNT_FILE_NAME"]}'
        blobfilepath = blobfilepath.replace('//', '/')
        print(f"blobfilepath: {blobfilepath}")
        return blobfilepath


    def create_blob_client(self):
        """create azure storage account blob client"""
        return self.create_blob_service_client().get_blob_client(
            container = self.config["AZURE_STORAGE_ACCOUNT_CONTAINER"],
            blob = check_str_for_substr_and_replace(self.get_blob_file_path(), "//")
        )
    
    
    def create_container(self, containername = None):
        """create azure storage account container"""
        # try:
        self.create_blob_service_client().create_container(containername)
        print(f"azure storage account container created successfully: {containername}\n")
        # except: print(f"create azure storage account container failed: container {containername} already exists...\n")


    def delete_container(self, containername = None):
        """delete azure storage account container"""
        try:
            self.create_blob_service_client().delete_container(containername)
            print(f"azure storage account container and all files deleted successfully: {containername}\n")
        except: print(f"delete azure storage account container and all files failed: container {containername} does not exist...\n")


    def get_blob_list(self, sas_token = None):
        """get list of blobs in azure storage account container"""
        if sas_token != None: # use sas token for auth
            return self.create_container_client(sas_token).list_blobs()
        return self.create_container_client().list_blobs()


    def upload_blob(self, localfilepath, blobfilepath, overwrite = False):
        """upload a blob to an azure storage account container"""
        with open(localfilepath, "rb") as data:
           self.create_container_client().upload_blob(name = blobfilepath, data = data, overwrite = overwrite)


    def delete_blob(self):
        """delete blob from azure storage account"""
        self.create_blob_client().delete_blob()


    def download_blob(self):
        """download a blob from azure storage account container"""
        return self.create_blob_client().download_blob()


    def listblobfiles(self, storageacctname = None, container = None, folderpath = None, sas_token = None):
        """list specific blob files in an azure storage account container"""
        self.set_azure_storage_acct_name_override(storageacctname)
        self.set_azure_storage_acct_container_name_override(container)
        files = self.get_blob_list(sas_token)
        filelist = []
        for file in files:
            if file.name.startswith(folderpath):
                filelist.append(file.name)
        return filelist


    def download_blob_write_locally(self, storageacctname = None, container = None, folderpath = None, subfolderpath = None, filename = None, data_sas = None):
        """
        download azure storage container blob and maintain blob folder structure locally
        return local file path each time this function is called
        """
        self.set_azure_storage_acct_name_override(storageacctname)
        self.set_azure_storage_acct_container_name_override(container)
        self.set_azure_storage_acct_folder_path_override(folderpath)
        self.set_azure_storage_acct_subfolder_path_override(subfolderpath)
        self.set_azure_storage_acct_file_name_override(filename)
        localpath = check_str_for_substr_and_replace(f'./{self.config["LOCAL_DATA_FOLDER"]}/azurestorage/{storageacctname}/{container}/{folderpath}', "//")
        print(f"bloblocalpath: {localpath}")
        if not os.path.exists(localpath): os.makedirs(localpath)
        localfilepath = f"{localpath}/{filename}"
        with open(localfilepath, "wb") as my_blob:
            if data_sas == None:
                blob_data = self.download_blob()
                blob_data.readinto(my_blob)
            else: my_blob.write(data_sas)
        print(f"{localfilepath} written locally successfully....\n")
        return localfilepath


    def upload_blob_from_local(self, storageacctname = None, container = None, localfilepath = None, blobfilepath = None, overwrite = False):
        """upload local file to azure storage account container and maintain local folder structure"""
        
        if len(container) < 3: container = container + "-addedchars" # added chars
        if len(container) > 24: container = container[:24] # take first 24 characters
        # remove invalid characters and fix case on the az container (e.g. no capital letters, no commas, no periods)
        # lowercase = True, uppercase = False, removenumbers = False, removespaces = True, removepunctuation = True, singledashes = True
        container = remove_invalid_chars(container, True, False, False, True, True, True)

        self.set_azure_storage_acct_name_override(storageacctname)
        self.set_azure_storage_acct_container_name_override(container)
        # ensure the container exists in the azure storage account
        self.create_container(container)
        self.upload_blob(localfilepath, blobfilepath, overwrite)
        print(f"{localfilepath} uploaded to azure storage account {storageacctname}/{container}: {blobfilepath} successfully....\n")
