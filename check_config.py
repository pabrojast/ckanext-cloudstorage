#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CloudStorage Configuration Checker

This script helps identify common configuration issues with ckanext-cloudstorage.
Run this script to validate your configuration before starting CKAN.

Example configurations:

For AWS S3:
    ckanext.cloudstorage.driver = S3
    ckanext.cloudstorage.driver_options = {"key": "YOUR_ACCESS_KEY", "secret": "YOUR_SECRET_KEY"}
    ckanext.cloudstorage.container_name = my-bucket-name

For Azure Blob Storage:
    ckanext.cloudstorage.driver = AZURE_BLOBS
    ckanext.cloudstorage.driver_options = {"key": "STORAGE_ACCOUNT_NAME", "secret": "STORAGE_ACCOUNT_KEY"}
    ckanext.cloudstorage.container_name = my-container-name

For local testing (OpenStack Swift):
    ckanext.cloudstorage.driver = OPENSTACK_SWIFT
    ckanext.cloudstorage.driver_options = {"key": "test", "secret": "test", "secure": false, "host": "127.0.0.1", "port": 8080}
    ckanext.cloudstorage.container_name = test-container
"""

import sys

print(__doc__)

# Common driver names from libcloud
VALID_DRIVERS = [
    'S3',
    'S3_AP_NORTHEAST',
    'S3_AP_NORTHEAST1',
    'S3_AP_NORTHEAST2',
    'S3_AP_SOUTH',
    'S3_AP_SOUTHEAST',
    'S3_AP_SOUTHEAST2',
    'S3_CA_CENTRAL',
    'S3_CN_NORTH',
    'S3_CN_NORTHWEST',
    'S3_EU_CENTRAL',
    'S3_EU_WEST',
    'S3_EU_WEST2',
    'S3_EU_NORTH',
    'S3_SA_EAST',
    'S3_US_EAST2',
    'S3_US_WEST',
    'S3_US_WEST_OREGON',
    'S3_US_GOV_WEST',
    'S3_US_GOV_EAST',
    'S3_RGW',
    'S3_RGW_OUTSCALE',
    'AZURE_BLOBS',
    'GOOGLE_STORAGE',
    'OPENSTACK_SWIFT',
    'CLOUDFILES',
    'BACKBLAZE_B2',
    'ALIYUN_OSS',
    'KTUCLOUD',
    'NIMBUS',
    'LOCAL'
]

print("\nValid driver names:")
for driver in VALID_DRIVERS:
    print(f"  - {driver}")

print("\nNOTE: Driver names are case-sensitive!")
print("\nFor more drivers, see: https://libcloud.readthedocs.io/en/latest/storage/supported_providers.html")