# Databricks notebook source
# azure class (main-class)
class azureclass:

    # class constructor    
    def __init__(self, config):
        # get all configuration variables
        self.config = config


    def azure_tenant_override(self, azure_tenant_id = None):
        """set a new azure tenant id"""
        self.config["AZURE_TENANT_ID"] = azure_tenant_id

    
    def azure_subscription_override(self, azure_subscription_id = None):
        """set a new azure subscription id"""
        self.config["AZURE_SUBSCRIPTION_ID"] = azure_subscription_id


    def azure_resource_group_name_override(self, azure_resource_group_name = None):
        """set a new azure resource group name"""
        self.config["AZURE_RESOURCER_GROUP_NAME"] = azure_resource_group_name


    def azure_client_id_override(self, azure_client_id = None):
        """set a new azure client id (e.g. service principle)"""
        self.config["AZURE_CLIENT_ID"] = azure_client_id
    

    def azure_client_secret_override(self, azure_client_secret = None):
        """set a new azure client id (e.g. service principle password)"""
        self.config["AZURE_CLIENT_SECRET"] = azure_client_secret
