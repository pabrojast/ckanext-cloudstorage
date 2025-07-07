#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckan import plugins
import os.path
from ckanext.cloudstorage import storage
from ckanext.cloudstorage import helpers
import ckanext.cloudstorage.logic.action.multipart as m_action
import ckanext.cloudstorage.logic.auth.multipart as m_auth
import ckanext.cloudstorage.logic.action.azure_direct as azure_action
import ckanext.cloudstorage.logic.auth.azure_direct as azure_auth
from ckanext.cloudstorage import views
import logging

log = logging.getLogger(__name__)

if plugins.toolkit.check_ckan_version(min_version='2.9.0'):
    from ckanext.cloudstorage.plugin.flask_plugin import MixinPlugin
else:
    from ckanext.cloudstorage.plugin.pylons_plugin import MixinPlugin


class CloudStoragePlugin(MixinPlugin, plugins.SingletonPlugin):
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IDatasetForm, inherit=True)

    # IConfigurer

    def update_config(self, config):
        plugins.toolkit.add_template_directory(config, 'templates')
        plugins.toolkit.add_resource('fanstatic/scripts', 'cloudstorage-js')

    # ITemplateHelpers

    def get_helpers(self):
        return dict(
            cloudstorage_use_secure_urls=helpers.use_secure_urls,
            cloudstorage_use_azure_direct_upload=helpers.use_azure_direct_upload,
            cloudstorage_get_cloud_storage_type=helpers.get_cloud_storage_type,
            cloudstorage_use_enhanced_upload=helpers.use_enhanced_upload
        )

    def configure(self, config):
        log.info("CloudStoragePlugin: Starting configuration")
        
        required_keys = (
            'ckanext.cloudstorage.driver',
            'ckanext.cloudstorage.driver_options',
            'ckanext.cloudstorage.container_name'
        )

        for rk in required_keys:
            value = config.get(rk)
            if value is None:
                log.error(f"CloudStoragePlugin: Missing required configuration: {rk}")
                raise RuntimeError(
                    'Required configuration option {0} not found.'.format(
                        rk
                    )
                )
            else:
                log.info(f"CloudStoragePlugin: Found config {rk} = {value[:50]}..." if len(str(value)) > 50 else f"CloudStoragePlugin: Found config {rk} = {value}")

    def get_resource_uploader(self, data_dict):
        # We provide a custom Resource uploader.
        try:
            log.debug(f"CloudStoragePlugin: Creating ResourceCloudStorage uploader")
            uploader = storage.ResourceCloudStorage(data_dict)
            log.debug(f"CloudStoragePlugin: Successfully created uploader")
            return uploader
        except Exception as e:
            log.error(f"CloudStoragePlugin: Failed to create uploader: {e}")
            raise

    def get_uploader(self, upload_to, old_filename=None):
        # We don't provide misc-file storage (group images for example)
        # Returning None here will use the default Uploader.
        return None

    # IActions

    def get_actions(self):
        return {
            'cloudstorage_initiate_multipart': m_action.initiate_multipart,
            'cloudstorage_upload_multipart': m_action.upload_multipart,
            'cloudstorage_finish_multipart': m_action.finish_multipart,
            'cloudstorage_abort_multipart': m_action.abort_multipart,
            'cloudstorage_check_multipart': m_action.check_multipart,
            'cloudstorage_clean_multipart': m_action.clean_multipart,
            'cloudstorage_generate_azure_direct_upload_url': azure_action.generate_azure_direct_upload_url,
            'cloudstorage_confirm_azure_direct_upload': azure_action.confirm_azure_direct_upload,
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'cloudstorage_initiate_multipart': m_auth.initiate_multipart,
            'cloudstorage_upload_multipart': m_auth.upload_multipart,
            'cloudstorage_finish_multipart': m_auth.finish_multipart,
            'cloudstorage_abort_multipart': m_auth.abort_multipart,
            'cloudstorage_check_multipart': m_auth.check_multipart,
            'cloudstorage_clean_multipart': m_auth.clean_multipart,
            'cloudstorage_generate_azure_direct_upload_url': azure_auth.generate_azure_direct_upload_url,
            'cloudstorage_confirm_azure_direct_upload': azure_auth.confirm_azure_direct_upload,
        }

    # IResourceController

    def before_delete(self, context, resource, resources):
        # let's get all info about our resource. It somewhere in resources
        # but if there is some possibility that it isn't(magic?) we have
        # `else` clause

        for res in resources:
            if res['id'] == resource['id']:
                break
        else:
            return
        # just ignore simple links
        if res['url_type'] != 'upload':
            return

        # we don't want to change original item from resources, just in case
        # someone will use it in another `before_delete`. So, let's copy it
        # and add `clear_upload` flag
        res_dict = res.copy()
        res_dict.update([('clear_upload', True)])

        uploader = self.get_resource_uploader(res_dict)

        # to be on the safe side, let's check existence of container
        container = getattr(uploader, 'container', None)
        if container is None:
            return

        # and now uploader removes our file.
        uploader.upload(resource['id'])

        # and all other files linked to this resource
        if not uploader.leave_files:
            upload_path = os.path.dirname(
                uploader.path_from_filename(
                    resource['id'],
                    'fake-name'
                )
            )

            for old_file in uploader.container.iterate_objects():
                if old_file.name.startswith(upload_path):
                    old_file.delete()

    # IDatasetForm

    def is_fallback(self):
        # Return True to handle all package types as a fallback
        return True

    def package_types(self):
        # Return empty list when acting as fallback
        # This allows us to handle all package types not explicitly claimed
        return []
