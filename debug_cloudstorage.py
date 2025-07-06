#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Debug script to test CloudStorage initialization
Run this to identify configuration issues
"""

import sys
import traceback

# Try to import and test CloudStorage initialization
try:
    from ckan.plugins.toolkit import config
    from ckanext.cloudstorage.storage import CloudStorage, ResourceCloudStorage
    from ast import literal_eval
    
    print("=== CloudStorage Debug Tool ===\n")
    
    # Check required configuration keys
    required_keys = [
        'ckanext.cloudstorage.driver',
        'ckanext.cloudstorage.driver_options',
        'ckanext.cloudstorage.container_name'
    ]
    
    print("1. Checking configuration keys:")
    for key in required_keys:
        value = config.get(key)
        if value:
            print(f"   ✓ {key} = {value[:50]}..." if len(str(value)) > 50 else f"   ✓ {key} = {value}")
        else:
            print(f"   ✗ {key} = NOT SET")
    
    # Try to parse driver_options
    print("\n2. Parsing driver_options:")
    try:
        driver_options_str = config.get('ckanext.cloudstorage.driver_options', '{}')
        print(f"   Raw value: {driver_options_str}")
        driver_options = literal_eval(driver_options_str)
        print(f"   Parsed successfully: {type(driver_options)} with keys: {list(driver_options.keys())}")
    except Exception as e:
        print(f"   ✗ ERROR parsing driver_options: {e}")
        print(f"   Make sure driver_options is a valid Python dict string, e.g.: '{\"key\": \"value\"}'")
    
    # Try to initialize CloudStorage
    print("\n3. Initializing CloudStorage:")
    try:
        storage = CloudStorage()
        print("   ✓ CloudStorage initialized successfully")
        print(f"   Driver: {storage.driver_name}")
        print(f"   Container: {storage.container_name}")
        print(f"   Driver class: {storage.driver}")
    except Exception as e:
        print(f"   ✗ ERROR initializing CloudStorage: {e}")
        traceback.print_exc()
    
    # Try to initialize ResourceCloudStorage
    print("\n4. Initializing ResourceCloudStorage:")
    try:
        resource_storage = ResourceCloudStorage({})
        print("   ✓ ResourceCloudStorage initialized successfully")
    except Exception as e:
        print(f"   ✗ ERROR initializing ResourceCloudStorage: {e}")
        traceback.print_exc()
    
    # Check for enhanced upload
    print("\n5. Checking enhanced upload configuration:")
    enhanced_upload = config.get('ckanext.cloudstorage.use_enhanced_upload', 'false')
    print(f"   Enhanced upload: {enhanced_upload}")
    
    # Check for Azure direct upload
    print("\n6. Checking Azure direct upload:")
    azure_direct = config.get('ckanext.cloudstorage.azure_direct_upload', 'false')
    print(f"   Azure direct upload: {azure_direct}")
    
    print("\n=== Debug Complete ===")
    
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running this from the CKAN environment")
except Exception as e:
    print(f"Unexpected error: {e}")
    traceback.print_exc()