#!/usr/bin/env python3
"""
create_akv_scope.py
Create a Databricks secret scope backed by Azure Key Vault.

Prerequisites
-------------
* Azure CLI (`az`) installed and you are logged in with `az login`
* Your Azure AD user has:
  • Databricks workspace “Can Manage” permission
  • Access policy on the Key Vault with “Get” and “List” secret permissions
* Python 3.8+

Usage
-----
python create_akv_scope.py \
    --workspace-url https://adb-12345678901234.16.azuredatabricks.net \
    --scope-name my-akv-scope \
    --kv-dns https://my-keyvault.vault.azure.net/ \
    --kv-resource-id /subscriptions/xxxx-xxxx-xxxx-xxxx/resourceGroups/rg/providers/Microsoft.KeyVault/vaults/my-keyvault
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

import requests

# ---------- Helpers -----------------------------------------------------------

def get_aad_token() -> str:
    """
    Fetch a user AAD token for the Databricks resource.
    Uses: az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d
    """
    try:
        result = subprocess.run(
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as err:
        print("❌ Failed to obtain AAD token via Azure CLI.")
        print(err.stderr)
        sys.exit(1)


def create_scope(
    workspace_url: str, scope_name: str, kv_dns: str, kv_resource_id: str, aad_token: str
):
    url = f"{workspace_url.rstrip('/')}/api/2.0/secrets/scopes/create"
    payload = {
        "scope": scope_name,
        "scope_backend_type": "AZURE_KEYVAULT",
        "backend_azure_keyvault": {
            "dns_name": kv_dns,
            "resource_id": kv_resource_id,
        },
    }
    headers = {
        "Authorization": f"Bearer {aad_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    if response.status_code == 200:
        print(f"✅ Secret scope “{scope_name}” created successfully.")
    else:
        print(f"❌ Failed to create scope (HTTP {response.status_code})")
        print(json.dumps(response.json(), indent=2))
        sys.exit(1)


# ---------- Main --------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Create AKV-backed Databricks secret scope")
    parser.add_argument("--workspace-url", required=True, help="Databricks workspace URL")
    parser.add_argument("--scope-name", required=True, help="Name of the secret scope to create")
    parser.add_argument("--kv-dns", required=True, help="Key Vault DNS name, e.g. https://kv.vault.azure.net/")
    parser.add_argument("--kv-resource-id", required=True, help="Full Azure resource ID of the Key Vault")
    args = parser.parse_args()

    token = get_aad_token()
    create_scope(args.workspace_url, args.scope_name, args.kv_dns, args.kv_resource_id, token)


if __name__ == "__main__":
    main()
