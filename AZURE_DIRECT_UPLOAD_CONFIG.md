# Azure Direct Upload Configuration

To enable direct uploads to Azure Blob Storage and avoid gateway timeout errors, add the following configuration to your CKAN configuration file (e.g., `production.ini`, `development.ini`, etc.):

## Required Configuration

```ini
# Enable Azure direct upload feature
ckanext.cloudstorage.azure_direct_upload = 1

# Make sure you have Azure as your driver
ckanext.cloudstorage.driver = AZURE_BLOBS
ckanext.cloudstorage.container_name = your-container-name

# Azure credentials
ckanext.cloudstorage.driver_options = {"key": "<storage_account_name>", "secret": "<storage_account_key>"}

# Optional: Enable secure URLs (recommended)
ckanext.cloudstorage.use_secure_urls = 1
```

## How It Works

When `ckanext.cloudstorage.azure_direct_upload = 1` is enabled:

1. Files upload directly to Azure Blob Storage using SAS (Shared Access Signature) tokens
2. The upload bypasses the CKAN gateway, preventing timeout errors on large files
3. The frontend uses a specialized JavaScript module (`cloudstorage-azure-direct-upload.js`)
4. Upload flow:
   - User selects a file
   - CKAN generates a temporary SAS token (1-hour expiry)
   - File uploads directly from browser to Azure
   - Upload confirmation is sent back to CKAN

## Requirements

Make sure you have the required Azure package installed:
```bash
pip install azure-storage-blob==12.2.0
```

This is already included in the `requirements.txt` file.

## Verification

After adding the configuration and restarting CKAN, you can verify it's working by:
1. Going to a dataset and clicking "Add Resource"
2. The upload should go directly to Azure (you'll see progress without gateway timeouts)
3. Check the browser's Network tab - uploads should go to `*.blob.core.windows.net` instead of your CKAN server

## Troubleshooting

If uploads are still timing out:
1. Verify `ckanext.cloudstorage.azure_direct_upload = 1` is set correctly
2. Check that `azure-storage-blob` is installed
3. Ensure your Azure credentials are correct
4. Check CKAN logs for any error messages