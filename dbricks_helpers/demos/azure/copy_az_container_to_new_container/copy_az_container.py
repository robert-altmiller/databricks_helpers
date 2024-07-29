# Databricks notebook source
# DBTITLE 1,Create a Copy of an Azure Container
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


def copy_container(
    source_connection_string = None, 
    source_container_name = None, 
    destination_connection_string = None, 
    destination_container_name = None,
    copy_files_filter = None
):  
    # create BlobServiceClient instances for the source and destination containers
    source_blob_service_client = BlobServiceClient.from_connection_string(source_connection_string)
    destination_blob_service_client = BlobServiceClient.from_connection_string(destination_connection_string)

    # get a reference to the source container
    source_container_client = source_blob_service_client.get_container_client(source_container_name)

    # create the destination container if it doesn't exist
    destination_container_client = destination_blob_service_client.get_container_client(destination_container_name)
    try: destination_container_client.create_container()
    except: print("container already exists....")

    # list blobs in the source container
    blobs = source_container_client.list_blobs()

    for blob in blobs:
    
        source_blob = source_container_client.get_blob_client(blob.name)

        if source_blob.get_blob_properties().size > 0:
            if copy_files_filter != None and copy_files_filter in source_blob.url:
                print(source_blob.url)
                destination_blob = destination_container_client.get_blob_client(blob.name)
                # copy the blob to the destination container
                destination_blob.start_copy_from_url(source_blob.url)
            else:
                print(source_blob.url)
                destination_blob = destination_container_client.get_blob_client(blob.name)
                # copy the blob to the destination container
                destination_blob.start_copy_from_url(source_blob.url)

    print(f"Container '{source_container_name}' copied to '{destination_container_name}' successfully.")

# storage account settings
src_conn_str = ""
src_container_name = ""
dest_conn_str = ""
dest_container_name = ""

# copy container
copy_container(
    source_connection_string = src_conn_str, 
    source_container_name = src_container_name, 
    destination_connection_string = dest_conn_str, 
    destination_container_name = dest_container_name, 
    copy_files_filter = None # "archive/General-Accounting"
)
