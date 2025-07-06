# Enhanced Upload Feature

The enhanced upload feature provides an improved file upload interface with support for multiple files, drag-and-drop, and better progress tracking.

## Features

- **Multiple File Selection**: Upload multiple files at once
- **Drag & Drop**: Drag files directly onto the upload area
- **Upload Queue**: Manage files before uploading
- **Progress Tracking**: Individual and overall progress bars
- **Azure Direct Upload**: Full support for direct uploads to Azure
- **Graceful Fallback**: Works with multipart and standard uploads

## Configuration

To enable the enhanced upload interface globally, add this to your CKAN configuration:

```ini
ckanext.cloudstorage.use_enhanced_upload = true
```

## Usage

### Option 1: Global Configuration
When `ckanext.cloudstorage.use_enhanced_upload = true` is set, all resource upload forms will automatically use the enhanced interface.

### Option 2: Per-Template Usage
You can enable it for specific templates by passing `use_enhanced_upload=True`:

```jinja2
{% snippet 'cloudstorage/snippets/multipart_module.html', 
           pkg_name=pkg_name, 
           parent=super, 
           use_enhanced_upload=True %}
```

### Option 3: Custom Template
Use the provided example template:
```jinja2
{% extends "package/new_resource_enhanced.html" %}
```

## JavaScript API

The enhanced upload module (`cloudstorage-enhanced-upload`) provides these features:

- Automatic file type validation
- File size limits (5GB default)
- Queue management
- Concurrent upload support (with Azure direct upload)
- Real-time progress updates

## Styling

The interface includes:
- Modern drag-and-drop zone
- Queue list with file information
- Individual progress bars per file
- Overall progress summary
- Status indicators (pending, uploading, success, error)

## Browser Compatibility

- Modern browsers with HTML5 File API support
- Drag and drop requires HTML5 DnD API
- Progress tracking requires XMLHttpRequest Level 2

## Example Configuration

```ini
# Enable Azure direct upload (recommended for large files)
ckanext.cloudstorage.azure_direct_upload = 1

# Enable enhanced upload interface
ckanext.cloudstorage.use_enhanced_upload = true

# Standard Azure configuration
ckanext.cloudstorage.driver = AZURE_BLOBS
ckanext.cloudstorage.container_name = your-container
ckanext.cloudstorage.driver_options = {"key": "account_name", "secret": "account_key"}
```

## Notes

- When uploading multiple files, each file creates a separate resource in CKAN
- The URL field is disabled during file uploads
- Failed uploads can be retried individually
- The queue persists until manually cleared or page refresh