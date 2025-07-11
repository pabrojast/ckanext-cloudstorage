# ckanext-cloudstorage

Implements support for using S3, Azure, or any of 15 different storage
providers supported by [libcloud][] to [CKAN][].

# Setup

After installing `ckanext-cloudstorage`, add it to your list of plugins in
your `.ini`:

    ckan.plugins = stats cloudstorage

If you haven't already, setup [CKAN file storage][ckanstorage] or the file
upload button will not appear.

## Basic Configuration

Every driver takes two options, regardless of which one you use. Both
the name of the driver and the name of the container/bucket are
case-sensitive:

    ckanext.cloudstorage.driver = AZURE_BLOBS
    ckanext.cloudstorage.container_name = demo

You can find a list of driver names [here][storage] (see the `Provider
Constant` column.)

Each driver takes its own setup options. See the [libcloud][] documentation.
These options are passed in using `driver_options`, which is a Python dict.
For most drivers, this is all you need:

    ckanext.cloudstorage.driver_options = {"key": "<your public key>", "secret": "<your secret key>"}

## Advanced Configuration Options

### Security and URLs

**`ckanext.cloudstorage.use_secure_urls`** (Value: `0` or `1`)
```ini
ckanext.cloudstorage.use_secure_urls = 1
```
**What it does:**
- **`1` (Enabled)**: Generates temporary secure URLs with access tokens that expire in 1 hour
- **`0` (Disabled)**: Uses direct public URLs to files in the storage provider

**Benefits when enabled:**
- 🔒 **Enhanced security**: Only authorized users can download files
- ⏱️ **Temporary URLs**: Links automatically expire (1 hour)
- 🔐 **Access control**: Respects CKAN permission restrictions
- 🚀 **Multipart uploads**: Enables large file uploads in chunks

**Supported providers:** Azure Blob Storage, AWS S3

### Upload Enhancement

**`ckanext.cloudstorage.use_enhanced_upload`** (Value: `0` or `1`)
```ini
ckanext.cloudstorage.use_enhanced_upload = 1
```
**What it does:**
- **`1` (Enabled)**: Enables enhanced file upload interface
- **`0` (Disabled)**: Uses standard CKAN interface

**Benefits when enabled:**
- 📊 **Visual indicators**: Shows upload status and progress
- ✅ **Enhanced validation**: File type and size verification
- 🎯 **Better UX**: More intuitive and responsive interface
- 🔄 **Real-time feedback**: Status notifications during upload

### Azure Direct Upload

**`ckanext.cloudstorage.azure_direct_upload`** (Value: `0` or `1`)
```ini
ckanext.cloudstorage.azure_direct_upload = 1
```
**What it does:**
- **`1` (Enabled)**: Allows direct uploads from browser to Azure Blob Storage
- **`0` (Disabled)**: Files are uploaded through the CKAN server

**Benefits when enabled:**
- 🚀 **Speed**: Direct upload without going through CKAN server
- 💾 **Efficiency**: Reduces load on CKAN server
- 📡 **Bandwidth**: Saves server bandwidth
- ⚡ **Scalability**: Better performance for large files

**Requirement:** Only works with Azure Blob Storage

### Complete Configuration Example

```ini
# Basic configuration
ckanext.cloudstorage.driver = AZURE_BLOBS
ckanext.cloudstorage.container_name = my-ckan-container
ckanext.cloudstorage.driver_options = {"key": "my_key", "secret": "my_secret"}

# Advanced configurations
ckanext.cloudstorage.use_secure_urls = 1
ckanext.cloudstorage.azure_direct_upload = 1
ckanext.cloudstorage.use_enhanced_upload = 1

# Additional configurations
ckanext.cloudstorage.max_multipart_lifetime = 7
ckanext.cloudstorage.guess_mimetype = 1
ckanext.cloudstorage.leave_files = 0
```

### Additional Configurations

**`ckanext.cloudstorage.max_multipart_lifetime`** (Value in days)
```ini
ckanext.cloudstorage.max_multipart_lifetime = 7
```
Defines how many days to keep incomplete multipart uploads before automatically deleting them.

**`ckanext.cloudstorage.guess_mimetype`** (Value: `0` or `1`)
```ini
ckanext.cloudstorage.guess_mimetype = 1
```
Automatically determines the MIME type of uploaded files.

**`ckanext.cloudstorage.leave_files`** (Value: `0` or `1`)
```ini
ckanext.cloudstorage.leave_files = 0
```
- `0`: Deletes files from storage when resources are deleted in CKAN
- `1`: Keeps files in storage even if deleted from CKAN

# Support

Most libcloud-based providers should work out of the box, but only those listed
below have been tested:

| Provider | Uploads | Downloads | Secure URLs (private resources) |
| --- | --- | --- | --- |
| Azure    | YES | YES | YES (if `azure-storage` is installed) |
| AWS S3   | YES | YES | YES (if `boto` is installed) |
| Rackspace | YES | YES | No |

# What are "Secure URLs"?

"Secure URLs" are a method of preventing access to private resources. By
default, anyone that figures out the URL to your resource on your storage
provider can download it. Secure URLs allow you to disable public access and
instead let ckanext-cloudstorage generate temporary, one-use URLs to download
the resource. This means that the normal CKAN-provided access restrictions can
apply to resources with no further effort on your part, but still get all the
benefits of your CDN/blob storage.

    ckanext.cloudstorage.use_secure_urls = 1

This option also enables multipart uploads, but you need to create database tables
first. Run next command from extension folder:
    `paster cloudstorage initdb -c /etc/ckan/default/production.ini `

With that feature you can use `cloudstorage_clean_multipart` action, which is available
only for sysadmins. After executing, all unfinished multipart uploads, older than 7 days,
will be aborted. You can configure this lifetime, example:

     ckanext.cloudstorage.max_multipart_lifetime  = 7

# Migrating From FileStorage

If you already have resources that have been uploaded and saved using CKAN's
built-in FileStorage, cloudstorage provides an easy migration command.
Simply setup cloudstorage as explained above, enable the plugin, and run the
migrate command. Provide the path to your resources on-disk (the
`ckan.storage_path` setting in your CKAN `.ini` + `/resources`), and
cloudstorage will take care of the rest. Ex:

    paster cloudstorage migrate <path to files> -c ../ckan/development.ini

# Integration with ckanext-schemingdcat

This project includes complete integration with **ckanext-schemingdcat** to provide enhanced file uploads using cloudstorage within schemingdcat forms.

## What has been modified?

### SchemingDCAT Plugin (`SchemingDCATDatasetsPlugin`)
- ✅ **IUploader implemented**: Automatically uses `ResourceCloudStorage` for all uploads
- ✅ **Integrated helpers**: All cloudstorage helpers available in templates
- ✅ **Deletion handling**: Removes files from storage when resources are deleted
- ✅ **No conflicts**: Doesn't interfere with the original cloudstorage plugin

### Enhanced Templates
- ✅ **Resource forms**: Automatic cloudstorage integration in schemingdcat forms
- ✅ **Visual indicators**: Shows cloudstorage status to users
- ✅ **Robust fallbacks**: Works with or without cloudstorage configured
- ✅ **Enhanced JavaScript**: Real-time feedback during uploads

## Configuration

To use cloudstorage with schemingdcat, simply enable both plugins:

```ini
ckan.plugins = stats scheming_datasets scheming_groups scheming_organizations schemingdcat cloudstorage

# Cloudstorage configuration (same as before)
ckanext.cloudstorage.driver = AZURE_BLOBS
ckanext.cloudstorage.container_name = my-container
ckanext.cloudstorage.driver_options = {"key": "my_key", "secret": "my_secret"}
ckanext.cloudstorage.use_secure_urls = 1
ckanext.cloudstorage.use_enhanced_upload = 1
ckanext.cloudstorage.azure_direct_upload = 1
```

## Integration Benefits

### For Administrators
- 🔧 **Simple setup**: Just enable both plugins
- 📊 **Single system**: Cloudstorage handles all uploads automatically
- 🔄 **Easy migration**: No loss of schemingdcat functionality
- ⚙️ **Reduced maintenance**: Single storage system to maintain

### For Users
- 🎨 **Familiar interface**: Same schemingdcat forms with enhanced functionality
- 🚀 **Faster uploads**: Especially with Azure Direct Upload
- 📈 **Larger files**: Automatic multipart upload support
- 🔒 **Enhanced security**: Secure URLs if configured

## Available Features

| Feature | schemingdcat only | schemingdcat + cloudstorage |
|---|---|---|
| DCAT forms | ✅ | ✅ |
| Local upload | ✅ | ✅ (fallback) |
| Cloud upload | ❌ | ✅ |
| Secure URLs | ❌ | ✅ |
| Multipart uploads | ❌ | ✅ |
| Direct upload | ❌ | ✅ (Azure) |
| Visual indicators | Basic | ✅ Enhanced |

## Troubleshooting

### CloudStorage helpers not available
If you see messages like `CloudStorage helpers not available`, verify:
1. The `cloudstorage` plugin is in the plugins list
2. Cloudstorage configuration is correct
3. Plugins are in the correct order

### Assets not found
If you see asset errors, verify:
1. Both plugins are active
2. Webassets configuration is correct
3. Restart CKAN after configuration changes

### Uploads not working
If uploads fail:
1. Verify cloudstorage configuration
2. Check CKAN logs for specific errors
3. Ensure container/bucket exists and has correct permissions

# Notes

1. You should disable public listing on the cloud service provider you're
   using, if supported.
2. Currently, only resources are supported. This means that things like group
   and organization images still use CKAN's local file storage.
3. Integration with schemingdcat is automatic - no additional configuration required
4. Schemingdcat forms maintain all their original functionality

# FAQ

- *DataViews aren't showing my data!* - did you setup CORS rules properly on
  your hosting service? ckanext-cloudstorage can try to fix them for you automatically,
  run:

        paster cloudstorage fix-cors <list of your domains> -c=<CKAN config>

- *Can I use only cloudstorage without schemingdcat?* - Yes! Cloudstorage works independently.

- *Can I use only schemingdcat without cloudstorage?* - Yes! Integration is optional and doesn't break schemingdcat.

- *Do the forms look different?* - No, they maintain the same design with additional discrete indicators.

- *Help! I can't seem to get it working!* - send me a mail! tk@tkte.ch

[libcloud]: https://libcloud.apache.org/
[ckan]: http://ckan.org/
[storage]: https://libcloud.readthedocs.io/en/latest/storage/supported_providers.html
[ckanstorage]: http://docs.ckan.org/en/latest/maintaining/filestore.html#setup-file-uploads
