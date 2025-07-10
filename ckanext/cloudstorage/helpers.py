#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckanext.cloudstorage.storage import ResourceCloudStorage


def use_secure_urls():
    # Se habilita el uso de URLs seguras sin restringir al proveedor
    return ResourceCloudStorage.use_secure_urls.fget(None)
