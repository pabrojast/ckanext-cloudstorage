#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ckan.plugins.toolkit as toolkit


def generate_azure_direct_upload_url(context, data_dict):
    """Auth function for generating Azure direct upload URLs.
    
    Users need to have edit permissions on the resource to generate upload URLs.
    """
    resource_id = data_dict.get('resource_id')
    if not resource_id:
        return {'success': False}
    
    try:
        # Check if user can edit the resource
        resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
        return toolkit.check_access('resource_update', context, resource)
    except (toolkit.ObjectNotFound, toolkit.NotAuthorized):
        return {'success': False}


def confirm_azure_direct_upload(context, data_dict):
    """Auth function for confirming Azure direct uploads.
    
    Users need to have edit permissions on the resource to confirm uploads.
    """
    resource_id = data_dict.get('resource_id')
    if not resource_id:
        return {'success': False}
    
    try:
        # Check if user can edit the resource
        resource = toolkit.get_action('resource_show')(context, {'id': resource_id})
        return toolkit.check_access('resource_update', context, resource)
    except (toolkit.ObjectNotFound, toolkit.NotAuthorized):
        return {'success': False}