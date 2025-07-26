# Databricks notebook source
# MAGIC %run "./general_helpers"

# COMMAND ----------

# DBTITLE 1,Get Authoring Data
def get_authoring_data(base_dir="./input_data", contract_exists=False):
    """
    Initializes and loads input metadata files for authoring a data contract.
    Parameters:
        base_dir (str): The base input directory path.
        contract_exists (bool): Determines if a data contract already exists.
    Returns:
        dict: Dictionary with loaded JSON payloads keyed by metadata type.
    """
    input_paths = {
        "custom_dq_rules_input": f"data_quality_rules_input/data_quality_rules.json",
        "contract_metadata_input": f"contract_metadata_input/contract_metadata.json",
        "server_metadata_input": f"server_metadata_input/server_metadata.json",
        "support_channel_metadata_input": f"support_channel_metadata_input/support_channel_metadata.json",
        "sla_metadata_input": f"sla_metadata_input/sla_metadata.json",
        "team_metadata_input": f"team_metadata_input/team_metadata.json",
        "roles_metadata_input": f"roles_metadata_input/roles_metadata.json",
        "pricing_metadata_input": f"pricing_metadata_input/pricing_metadata.json",
        "schema_metadata_input": f"schema_metadata_input/schema_metadata.json",
    }
    inputs = {}
    for folder_name, file_path in input_paths.items():
        author_path = f"{base_dir}/{file_path}"
        if check_file_exists(author_path):
            print(f"{author_path}")
            if folder_name == "custom_dq_rules_input":
                with open(author_path, "r") as file:
                    raw_data = file.read()
                raw_data = " ".join(raw_data.split())
                inputs[folder_name] = read_json_file(json_str=raw_data)
            else:
                inputs[folder_name] = read_json_file(author_path)
        else:  # data contract and author_path do not exist
            inputs[folder_name] = None
    return inputs