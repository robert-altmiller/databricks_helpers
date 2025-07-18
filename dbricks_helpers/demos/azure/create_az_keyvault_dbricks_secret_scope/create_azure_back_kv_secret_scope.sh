az login
python3 test.py \
  --workspace-url https://adb-1802111099045897.17.azuredatabricks.net \
  --scope-name altmiller-scope \
  --kv-dns https://my-keyvault-alt.vault.azure.net/ \
  --kv-resource-id /subscriptions/05337431-9084-4076-9103-bd8f767aa5b8/resourceGroups/my-resource-group/providers/Microsoft.KeyVault/vaults/my-keyvault-alt
