# CloudStorage + SchemingDCAT Integration

This document describes how ckanext-cloudstorage integrates with ckanext-schemingdcat to provide cloud storage functionality within scheming-based resource forms.

## Overview

The integration allows schemingdcat forms to use cloudstorage's advanced upload features including:
- Multipart uploads for large files
- Azure direct uploads (bypassing gateway)
- Enhanced upload interface with drag-and-drop support
- Multiple file uploads

## How it Works

### 1. Resource Form Override

CloudStorage implements `IDatasetForm.resource_form()` to provide its own resource form template:

```python
def resource_form(self):
    return 'cloudstorage/snippets/resource_form.html'
```

This template extends schemingdcat's resource form, maintaining all scheming functionality.

### 2. Upload Field Enhancement

The integration works by overriding the schemingdcat upload form snippet. When schemingdcat renders a field with the `resource_url_upload` preset, it will use CloudStorage's enhanced version located at:

```
ckanext/cloudstorage/templates/schemingdcat/form_snippets/upload.html
```

This template:
- Detects the cloud storage configuration
- Wraps the upload field with appropriate JavaScript modules
- Includes necessary assets for multipart/Azure direct uploads
- Falls back to standard upload when cloud storage is not configured

### 3. Configuration

No additional configuration is needed beyond the standard cloudstorage setup. The integration is automatic when both extensions are enabled:

```ini
ckan.plugins = ... cloudstorage schemingdcat ...
```

### 4. Template Structure

```
ckanext-cloudstorage/
├── templates/
│   ├── cloudstorage/
│   │   └── snippets/
│   │       ├── resource_form.html          # Main resource form (extends schemingdcat)
│   │       ├── fileupload.html            # Enhanced upload interface
│   │       └── multipart_module.html      # Multipart upload wrapper
│   └── schemingdcat/
│       └── form_snippets/
│           └── upload.html                 # Override for schemingdcat upload fields
```

### 5. JavaScript Assets

The integration automatically includes the necessary JavaScript modules based on configuration:

- `cloudstorage-multipart-upload.js` - For S3 multipart uploads
- `cloudstorage-azure-direct-upload.js` - For Azure direct uploads
- `cloudstorage-enhanced-upload.js` - For the enhanced upload interface

These are registered as Fanstatic resources and included via `{% asset %}` tags.

## Usage

### Standard Multipart Upload

With secure URLs enabled for S3:
```ini
ckanext.cloudstorage.use_secure_urls = 1
```

The upload field will automatically support multipart uploads for large files.

### Azure Direct Upload

For Azure Blob Storage with direct uploads:
```ini
ckanext.cloudstorage.driver = AZURE_BLOBS
ckanext.cloudstorage.azure_direct_upload = 1
```

Files upload directly to Azure, bypassing the CKAN gateway.

### Enhanced Upload Interface

Enable the drag-and-drop interface with multiple file support:
```ini
ckanext.cloudstorage.use_enhanced_upload = true
```

## Customization

To customize the upload behavior for specific schemas:

1. Override the upload template for a specific schema by creating:
   ```
   templates/schemingdcat/[schema_name]/form_snippets/upload.html
   ```

2. Modify field behavior in your schema YAML:
   ```yaml
   - field_name: url
     preset: resource_url_upload
     upload_label: Custom Upload Label
   ```

## Troubleshooting

1. **Templates not loading**: Ensure cloudstorage is listed after schemingdcat in `ckan.plugins`
2. **JavaScript errors**: Check that all required assets are being loaded (view page source)
3. **Upload failures**: Verify cloud storage credentials and CORS configuration

## Technical Details

The integration leverages CKAN's template inheritance system:
1. CloudStorage registers its template directory
2. The resource form extends schemingdcat's form
3. The upload snippet override is found first due to template search order
4. JavaScript modules enhance the rendered HTML at runtime

This approach ensures compatibility with future schemingdcat updates while providing cloud storage functionality.