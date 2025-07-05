#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import mimetypes

import ckan.lib.helpers as h
import ckan.plugins.toolkit as toolkit
from ckan.lib import munge

from ckanext.cloudstorage.storage import ResourceCloudStorage

log = logging.getLogger(__name__)


def generate_azure_direct_upload_url(context, data_dict):
    """Generate a direct upload URL for Azure Blob Storage.
    
    This action generates a SAS token URL that allows the client to upload
    files directly to Azure Blob Storage, bypassing the gateway to avoid
    timeout errors.

    :param context: CKAN context dict
    :param data_dict: dict with required keys:
        resource_id: The resource ID
        filename: Original filename  
        file_size: Size of the file to upload
    :returns: Dict with upload URL and metadata
    :rtype: dict
    """
    h.check_access('cloudstorage_generate_azure_direct_upload_url', context, data_dict)
    
    resource_id = toolkit.get_or_bust(data_dict, 'resource_id')
    filename = toolkit.get_or_bust(data_dict, 'filename')
    file_size = toolkit.get_or_bust(data_dict, 'file_size')
    
    # Create storage instance
    uploader = ResourceCloudStorage({})
    
    # Check if Azure direct upload is enabled
    if not uploader.use_azure_direct_upload:
        raise toolkit.ValidationError("Azure direct upload is not enabled")
        
    if not uploader.can_use_advanced_azure:
        raise toolkit.ValidationError("Azure advanced features not available")
    
    # Munge the filename
    munged_filename = munge.munge_filename(filename)
    
    try:
        # Generate the direct upload URL
        upload_info = uploader.generate_azure_direct_upload_url(
            resource_id, munged_filename, file_size
        )
        
        # Add MIME type information
        content_type, _ = mimetypes.guess_type(munged_filename)
        if content_type:
            upload_info['headers']['Content-Type'] = content_type
        
        return upload_info
        
    except Exception as e:
        log.error("Failed to generate Azure direct upload URL: %s", str(e))
        raise toolkit.ValidationError("Failed to generate upload URL: %s" % str(e))


def confirm_azure_direct_upload(context, data_dict):
    """Confirm that a direct upload to Azure was successful.
    
    This action is called after the client has successfully uploaded a file
    directly to Azure to update the resource metadata and mark the upload as complete.

    :param context: CKAN context dict  
    :param data_dict: dict with required keys:
        resource_id: The resource ID
        filename: Original filename
        blob_path: The blob path in Azure storage
    :returns: Success confirmation
    :rtype: dict
    """
    h.check_access('cloudstorage_confirm_azure_direct_upload', context, data_dict)
    
    resource_id = toolkit.get_or_bust(data_dict, 'resource_id')
    filename = toolkit.get_or_bust(data_dict, 'filename')
    blob_path = toolkit.get_or_bust(data_dict, 'blob_path')
    
    try:
        # Update the resource to mark upload as complete
        resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
        
        # Update resource metadata
        resource.update({
            'url': munge.munge_filename(filename),
            'url_type': 'upload',
            'last_modified': h.datetime_now_iso(),
        })
        
        # Remove upload in progress flag if it exists
        resource.pop('upload_in_progress', None)
        
        # Update the resource
        toolkit.get_action('resource_update')(context, resource)
        
        log.info("Direct upload confirmed for resource %s, blob: %s", resource_id, blob_path)
        
        return {
            'success': True,
            'resource_id': resource_id,
            'blob_path': blob_path
        }
        
    except Exception as e:
        log.error("Failed to confirm Azure direct upload: %s", str(e))
        raise toolkit.ValidationError("Failed to confirm upload: %s" % str(e))