#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckanext.cloudstorage.storage import ResourceCloudStorage
import ckan.plugins as p


def use_secure_urls():
    # Se habilita el uso de URLs seguras sin restringir al proveedor
    return ResourceCloudStorage.use_secure_urls.fget(None)


def use_azure_direct_upload():
    """Check if Azure direct upload is enabled"""
    return p.toolkit.asbool(
        p.toolkit.config.get('ckanext.cloudstorage.azure_direct_upload', False)
    )


def get_cloud_storage_type():
    """Get the configured cloud storage driver type"""
    return p.toolkit.config.get('ckanext.cloudstorage.driver', 'S3')


def use_enhanced_upload():
    """Check if enhanced upload interface is enabled"""
    return p.toolkit.asbool(
        p.toolkit.config.get('ckanext.cloudstorage.use_enhanced_upload', False)
    )
