# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ckanext-cloudstorage is a CKAN extension that provides cloud storage capabilities for file uploads. It integrates with 15+ cloud storage providers (AWS S3, Azure Blob Storage, Rackspace, etc.) through Apache Libcloud, replacing CKAN's default local file storage.

## Development Commands

### CLI Commands (Modern CKAN 2.9+)
```bash
# Database operations
ckan cloudstorage initdb                    # Initialize multipart upload tables

# Migration commands  
ckan cloudstorage migrate <path_to_storage> [resource_id]  # Migrate local files to cloud
ckan cloudstorage migrate-file <path_to_file> <resource_id>  # Migrate single file

# Storage management
ckan cloudstorage list-unlinked-uploads [-o output.txt]     # List orphaned uploads
ckan cloudstorage remove-unlinked-uploads                   # Remove orphaned uploads
ckan cloudstorage list-missing-uploads [-o output.txt]      # List resources missing uploads
ckan cloudstorage list-linked-uploads [-o output.txt]       # List valid uploads

# Maintenance
ckan cloudstorage fix-cors <domain1> <domain2>              # Fix CORS configuration
ckan cloudstorage reguess-mimetypes [-r resource_id] [-v]   # Fix MIME types
```

### Legacy Paster Commands (CKAN <2.9)
```bash
# Same commands but with paster syntax:
paster cloudstorage initdb -c /path/to/config.ini
paster cloudstorage migrate <path> -c /path/to/config.ini
# etc.
```

### Installation and Setup
```bash
# Install extension
pip install -e .

# Add to CKAN configuration
# ckan.plugins = stats cloudstorage

# Required config (example for S3):
# ckanext.cloudstorage.driver = S3
# ckanext.cloudstorage.container_name = my-bucket  
# ckanext.cloudstorage.driver_options = {"key": "ACCESS_KEY", "secret": "SECRET_KEY"}

# Optional features:
# ckanext.cloudstorage.use_secure_urls = 1
# ckanext.cloudstorage.max_multipart_lifetime = 7
```

## Architecture Overview

### Plugin Architecture
- **Dual Implementation**: Supports both Flask (CKAN 2.9+) and Pylons (legacy) through separate plugin mixins
- **Interface Implementation**: Implements `IUploader`, `IConfigurable`, `IActions`, `ITemplateHelpers`, `IAuthFunctions`, `IResourceController`
- **Entry Points**: Main plugin at `ckanext.cloudstorage.plugin:CloudStoragePlugin`

### Core Components

1. **Storage Layer** (`storage.py`)
   - `CloudStorage`: Base storage abstraction using Apache Libcloud
   - `ResourceCloudStorage`: CKAN-specific uploader implementation
   - Multi-provider authentication (AWS IAM roles, boto3 sessions)

2. **Multipart Upload System**
   - **Models** (`model.py`): SQLAlchemy models for upload tracking
   - **API Actions** (`logic/action/multipart.py`): RESTful upload management
   - **Authorization** (`logic/auth/multipart.py`): Permission handling
   - **Frontend** (`fanstatic/scripts/`): JavaScript chunked upload interface

3. **Version Compatibility**
   - **Flask Plugin** (`plugin/flask_plugin.py`): Blueprint + Click commands
   - **Pylons Plugin** (`plugin/pylons_plugin.py`): Legacy routing + Paster commands

4. **Web Interface**
   - **Downloads** (`views/resource_download.py`): Flask Blueprint for secure downloads
   - **Templates** (`templates/`): Custom resource creation/editing forms
   - **Assets** (`fanstatic/`): JavaScript modules for upload functionality

### Configuration Pattern
The extension is heavily configuration-driven:
- **Driver Selection**: `ckanext.cloudstorage.driver` (case-sensitive provider names)
- **Credentials**: `ckanext.cloudstorage.driver_options` (Python dict)
- **Container**: `ckanext.cloudstorage.container_name` (bucket/container name)
- **Features**: `use_secure_urls`, `max_multipart_lifetime`

### Security Features
- **Secure URLs**: Generates temporary signed URLs for private resources
- **Access Control**: Integrates with CKAN's authorization system
- **Multi-Auth**: Supports various AWS authentication methods (IAM, boto3)

## Development Patterns

### Code Organization
- Plugin interfaces in `plugin/__init__.py` (main logic)
- Version-specific implementations in `plugin/flask_plugin.py` and `plugin/pylons_plugin.py`
- Utility functions centralized in `utils.py`
- Database models follow SQLAlchemy patterns with proper cascading

### Frontend Integration
- JavaScript modules extend CKAN's existing upload interface
- Progressive enhancement for multipart uploads
- Asset bundling through `fanstatic/scripts/webassets.yml`

### Testing and Migration
- Migration tools for existing CKAN installations
- Utilities for data validation and cleanup
- CORS management for cross-origin requests

## Key Files to Understand

- `plugin/__init__.py:CloudStoragePlugin` - Main plugin implementation
- `storage.py:ResourceCloudStorage` - Core upload logic  
- `model.py` - Database schema for multipart uploads
- `utils.py` - CLI command implementations
- `logic/action/multipart.py` - API endpoints for uploads
- `fanstatic/scripts/cloudstorage-multipart-upload.js` - Frontend upload handling

## Common Development Tasks

When working with this extension, you'll typically:
1. Initialize database tables with `initdb` command after enabling secure URLs
2. Use migration commands to move existing files to cloud storage
3. Configure CORS rules with `fix-cors` for DataViews functionality
4. Monitor and clean up orphaned uploads using list/remove commands
5. Debug MIME type issues with `reguess-mimetypes`