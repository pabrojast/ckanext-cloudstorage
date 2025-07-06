#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckanext.cloudstorage.storage import ResourceCloudStorage


def use_secure_urls():
    return all([
        ResourceCloudStorage.use_secure_urls.fget(None),
        # Currently implemented just AWS version
        'S3' in ResourceCloudStorage.driver_name.fget(None)
    ])


def use_azure_direct_upload():
    """Check if Azure direct upload is enabled and available."""
    try:
        # First check if required configuration exists
        from ckan.plugins.toolkit import config
        driver = config.get('ckanext.cloudstorage.driver')
        if not driver:
            return False
            
        # Only try to initialize storage if configuration exists
        storage = ResourceCloudStorage({})
        return all([
            storage.driver_name == 'AZURE_BLOBS',
            storage.can_use_advanced_azure,
            storage.use_azure_direct_upload
        ])
    except Exception as e:
        import logging
        log = logging.getLogger(__name__)
        log.debug(f"Error checking azure direct upload: {e}")
        return False


def get_cloud_storage_type():
    """Get the current cloud storage driver type."""
    try:
        # First check if required configuration exists
        from ckan.plugins.toolkit import config
        driver = config.get('ckanext.cloudstorage.driver')
        if not driver:
            return None
            
        # Only try to initialize storage if configuration exists
        storage = ResourceCloudStorage({})
        return storage.driver_name
    except Exception as e:
        import logging
        log = logging.getLogger(__name__)
        log.debug(f"Error getting cloud storage type: {e}")
        return None


def use_enhanced_upload():
    """Check if enhanced upload interface should be used.
    
    This can be controlled via config option:
    ckanext.cloudstorage.use_enhanced_upload = true
    """
    from ckan.common import config
    return config.get('ckanext.cloudstorage.use_enhanced_upload', 'false').lower() == 'true'
