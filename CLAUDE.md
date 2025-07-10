# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`ckanext-cloudstorage` is a CKAN extension that provides cloud storage capabilities for file uploads using multiple storage providers (S3, Azure Blobs, and 15+ providers via Apache libcloud). It replaces CKAN's default file storage with cloud-based solutions and supports features like secure URLs, multipart uploads, and migration from local storage.

## Commands

### Development and Testing
No standard test framework is configured in the main extension. Testing appears to be handled through CKAN's testing infrastructure.

### Extension Commands
The extension provides several paster commands for maintenance:

```bash
# Initialize database tables for multipart uploads
paster cloudstorage initdb -c /path/to/config.ini

# Migrate existing local files to cloud storage
paster cloudstorage migrate <path_to_storage> [resource_id] -c /path/to/config.ini

# Migrate a single file to cloud storage
paster cloudstorage migrate-file <path_to_file> <resource_id> -c /path/to/config.ini

# Fix CORS rules for cloud storage providers
paster cloudstorage fix-cors <domain1> <domain2> ... -c /path/to/config.ini

# List uploads in storage that don't match any resources
paster cloudstorage list-unlinked-uploads [-o output_file] -c /path/to/config.ini

# Remove orphaned uploads from storage
paster cloudstorage remove-unlinked-uploads -c /path/to/config.ini

# List resources missing their uploads in storage
paster cloudstorage list-missing-uploads [-o output_file] -c /path/to/config.ini

# List uploads that match existing resources
paster cloudstorage list-linked-uploads [-o output_file] -c /path/to/config.ini
```

## Architecture

### Plugin Structure
The extension follows CKAN's plugin architecture with version-specific implementations:

- **Main Plugin**: `ckanext.cloudstorage.plugin.CloudStoragePlugin` - Entry point that implements multiple CKAN interfaces
- **Version-specific Mixins**: 
  - Flask-based: `ckanext.cloudstorage.plugin.flask_plugin.MixinPlugin` (CKAN 2.9+)
  - Pylons-based: `ckanext.cloudstorage.plugin.pylons_plugin.MixinPlugin` (older CKAN versions)

### Core Components

- **Storage Backend**: `ckanext.cloudstorage.storage.ResourceCloudStorage` - Handles cloud storage operations
- **Commands**: `ckanext.cloudstorage.commands.PasterCommand` - CLI utilities for maintenance
- **Views**: `ckanext.cloudstorage.views` - Web interface components
- **Multipart Upload Logic**: 
  - Actions: `ckanext.cloudstorage.logic.action.multipart`
  - Auth: `ckanext.cloudstorage.logic.auth.multipart`

### Configuration Requirements
The plugin requires these configuration options:
- `ckanext.cloudstorage.driver` - Storage provider (e.g., AZURE_BLOBS, S3)
- `ckanext.cloudstorage.driver_options` - Provider-specific credentials (dict)
- `ckanext.cloudstorage.container_name` - Storage container/bucket name

### Dependencies
- **Core**: `apache-libcloud` (2.8.3) for multi-provider cloud storage
- **AWS**: `boto3` (1.17.112) for S3-specific features
- **Azure**: `azure-storage-blob` (12.2.0) for Azure-specific features
- **CKAN API**: `ckanapi` for integration

### Key Features
- **Resource Upload**: Replaces CKAN's default resource uploader
- **Secure URLs**: Temporary, authenticated download URLs for private resources
- **Multipart Uploads**: Large file upload support with resumable transfers
- **Migration Tools**: Commands to move existing local files to cloud storage
- **CORS Management**: Automatic CORS configuration for supported providers

### File Organization
- Plugin implementations in `ckanext/cloudstorage/plugin/`
- Frontend assets in `ckanext/cloudstorage/fanstatic/`
- Templates in `ckanext/cloudstorage/templates/`
- Database model in `ckanext/cloudstorage/model.py`
- Helper functions in `ckanext/cloudstorage/helpers.py`