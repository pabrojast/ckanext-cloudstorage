#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cgi
import mimetypes
import os.path
from six.moves.urllib.parse import urlparse
from ast import literal_eval
from datetime import datetime, timedelta
from time import time
from tempfile import SpooledTemporaryFile
import threading

from ckan.plugins.toolkit import config
from ckan import model
from ckan.lib import munge
from ckan.plugins.toolkit import get_action
import ckan.plugins as p

from libcloud.storage.types import Provider, ObjectDoesNotExistError
from libcloud.storage.providers import get_driver


from werkzeug.datastructures import FileStorage as FlaskFileStorage
ALLOWED_UPLOAD_TYPES = (cgi.FieldStorage, FlaskFileStorage)

import logging

log = logging.getLogger(__name__)

# Module-level cache for Azure Direct Upload temp paths (per-process fallback)
# Key: cache_key (temp_{uuid}), Value: temp_path or 'COMPLETE'
_azure_upload_cache = {}

# Cache to track completed Azure Direct Uploads (per-process memory cache)
# Key: temp_path, Value: final_path (for quick lookup within same process)
_azure_completed_moves = {}

# Lock for thread-safe cache access within a single process
_cache_lock = threading.Lock()

# Request context tracking to prevent multiple processing in same request
# Uses Flask's g object when available
_REQUEST_CONTEXT_KEY = '_cloudstorage_processed_uploads'


def _is_upload_processed_in_request(temp_path):
    """Check if this upload was already processed in the current request."""
    try:
        from flask import g
        if hasattr(g, _REQUEST_CONTEXT_KEY):
            return temp_path in getattr(g, _REQUEST_CONTEXT_KEY, set())
    except (RuntimeError, ImportError):
        pass
    return False


def _mark_upload_processed_in_request(temp_path):
    """Mark this upload as processed in the current request."""
    try:
        from flask import g
        if not hasattr(g, _REQUEST_CONTEXT_KEY):
            setattr(g, _REQUEST_CONTEXT_KEY, set())
        getattr(g, _REQUEST_CONTEXT_KEY).add(temp_path)
    except (RuntimeError, ImportError):
        pass


def _check_upload_status_db(temp_path):
    """
    Check upload status in database.
    Returns: (status, final_path) or (None, None) if not found.
    """
    try:
        from ckanext.cloudstorage.model import AzureUploadStatus
        status_record = AzureUploadStatus.get_by_temp_path(temp_path)
        if status_record:
            return status_record.status, status_record.final_path
    except Exception as e:
        log.debug(f"Could not check upload status in DB: {e}")
    return None, None


def _acquire_move_lock(temp_path, resource_id, final_path):
    """
    Try to acquire a lock to move the blob. Uses database for distributed locking.
    Returns: 'acquired', 'already_locked', or 'completed'
    """
    try:
        from ckanext.cloudstorage.model import AzureUploadStatus
        
        # Check if already completed
        status, existing_final_path = _check_upload_status_db(temp_path)
        if status == 'completed':
            return 'completed', existing_final_path
        if status == 'moving':
            return 'already_locked', existing_final_path
        
        # Try to acquire lock
        record, is_new = AzureUploadStatus.create_or_lock(temp_path, resource_id, final_path)
        if is_new:
            return 'acquired', None
        elif record and record.status == 'completed':
            return 'completed', record.final_path
        else:
            return 'already_locked', record.final_path if record else None
            
    except Exception as e:
        log.debug(f"Database lock not available, using memory cache: {e}")
        # Fall back to memory cache
        with _cache_lock:
            if temp_path in _azure_completed_moves:
                return 'completed', _azure_completed_moves[temp_path]
        return 'acquired', None


def _mark_move_completed_db(temp_path, final_path):
    """Mark the move as completed in the database."""
    try:
        from ckanext.cloudstorage.model import AzureUploadStatus
        AzureUploadStatus.mark_completed(temp_path, final_path)
    except Exception as e:
        log.debug(f"Could not mark completion in DB: {e}")
    
    # Also update memory cache
    with _cache_lock:
        _azure_completed_moves[temp_path] = final_path


def _mark_move_failed_db(temp_path):
    """Mark the move as failed in the database."""
    try:
        from ckanext.cloudstorage.model import AzureUploadStatus
        AzureUploadStatus.mark_failed(temp_path)
    except Exception as e:
        log.debug(f"Could not mark failure in DB: {e}")


def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


class CloudStorage(object):
    def __init__(self):
        self._driver_options = literal_eval(config['ckanext.cloudstorage.driver_options'])
        if 'S3' in self.driver_name and not self.driver_options:
            if self.aws_use_boto3_sessions:
                self.authenticate_with_aws_boto3()
            else:
                self.authenticate_with_aws()

        self.driver = get_driver(
            getattr(
                Provider,
                self.driver_name
            )
        )(**self.driver_options)
        self._container = None

    def path_from_filename(self, rid, filename):
        raise NotImplementedError

    def authenticate_with_aws(self):
        import requests
        r = requests.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/")
        role = r.text
        r = requests.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/" + role)
        credentials = r.json()
        self.driver_options = {'key': credentials['AccessKeyId'],
                               'secret': credentials['SecretAccessKey'],
                               'token': credentials['Token'],
                               'expires': credentials['Expiration']}

        self.driver = get_driver(
            getattr(
                Provider,
                self.driver_name
            )
        )(**self.driver_options)
        self._container = None

    def authenticate_with_aws_boto3(self):
        """
        TTL max 900 seconds for IAM role session
        https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html#id_roles_use_view-role-max-session
        """
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()
        self.driver_options = {'key': current_credentials.access_key,
                               'secret': current_credentials.secret_key,
                               'token': current_credentials.token,
                               'expires': datetime.fromtimestamp(time() + 900).strftime('%Y-%m-%dT%H:%M:%SZ')}

        self.driver = get_driver(
            getattr(
                Provider,
                self.driver_name
            )
        )(**self.driver_options)
        self._container = None

    @property
    def container(self):
        """
        Return the currently configured libcloud container.
        """
        if self.driver_options.get('expires'):
            expires = datetime.strptime(self.driver_options['expires'], "%Y-%m-%dT%H:%M:%SZ")
            if expires < datetime.utcnow():
                if self.aws_use_boto3_sessions:
                    self.authenticate_with_aws_boto3()
                else:
                    self.authenticate_with_aws()

        if self._container is None:
            self._container = self.driver.get_container(
                container_name=self.container_name
            )

        return self._container

    @property
    def driver_options(self):
        """
        A dictionary of options ckanext-cloudstorage has been configured to
        pass to the apache-libcloud driver.
        """
        return self._driver_options

    @driver_options.setter
    def driver_options(self, value):
        self._driver_options = value

    @property
    def driver_name(self):
        """
        The name of the driver (ex: AZURE_BLOBS, S3) that ckanext-cloudstorage
        is configured to use.


        .. note::

            This value is used to lookup the apache-libcloud driver to use
            based on the Provider enum.
        """
        return config['ckanext.cloudstorage.driver']

    @property
    def container_name(self):
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        return config['ckanext.cloudstorage.container_name']

    @property
    def use_secure_urls(self):
        """
        `True` if ckanext-cloudstroage is configured to generate secure
        one-time URLs to resources, `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.use_secure_urls', False)
        )


    @property
    def connection_link(self):
        """
        The connection link to the container
        """
        return "AccountName={};AccountKey={}".format(
            self.driver_options['key'],
            self.driver_options['secret']
        )


    @property
    def aws_use_boto3_sessions(self):
        """
        'True' if ckanext-cloudstorage is configured to use boto3 instead of
        boto for AWS IAM sessions. This makes session creation possible on
        platforms like AWS ECS Fargate or AWS Lambda.
        """
        return bool(int(config.get('ckanext.cloudstorage.aws_use_boto3_sessions', 0)))

    @property
    def leave_files(self):
        """
        `True` if ckanext-cloudstorage is configured to leave files on the
        provider instead of removing them when a resource/package is deleted,
        otherwise `False`.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.leave_files', False)
        )

    @property
    def can_use_advanced_azure(self):
        """
        `True` if the `azure-storage` module is installed and
        ckanext-cloudstorage has been configured to use Azure, otherwise
        `False`.
        """
        # Are we even using Azure?
        if self.driver_name == 'AZURE_BLOBS':
            try:
                # Yes? Is the azure-storage package available?
                from azure import storage
                # Shut the linter up.
                assert storage
                return True
            except ImportError:
                pass

        return False

    @property
    def can_use_advanced_aws(self):
        """
        `True` if the `boto` module is installed and ckanext-cloudstorage has
        been configured to use Amazon S3, otherwise `False`.
        """
        # Are we even using AWS?
        if 'S3' in self.driver_name:
            try:
                # Yes? Is the boto package available?
                import boto
                # Shut the linter up.
                assert boto
                return True
            except ImportError:
                pass

        return False

    @property
    def guess_mimetype(self):
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.guess_mimetype', False)
        )


class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
        """
        Support for uploading resources to any storage provider
        implemented by the apache-libcloud library.

        :param resource: The resource dict.
        """
        super(ResourceCloudStorage, self).__init__()

        self.filename = None
        self.old_filename = None
        self.file = None
        self.resource = resource

        upload_field_storage = resource.pop('upload', None)
        self._clear = resource.pop('clear_upload', None)
        multipart_name = resource.pop('multipart_name', None)
        
        # Support for Azure Direct Upload - file already uploaded to temp path
        azure_blob_path = resource.pop('azure_blob_path', None)
        azure_upload = resource.pop('azure_upload', None)
        self.azure_temp_path = None
        self.azure_direct_complete = False  # Flag for existing resources where file is already in final location
        
        # Check module-level cache first (for when CKAN creates a new uploader instance for upload())
        # Priority: 1) Use _azure_cache_key if available, 2) Check url for existing COMPLETE uploads
        resource_url = resource.get('url', '')
        saved_cache_key = resource.get('_azure_cache_key')
        
        log.info(f"ResourceCloudStorage init: Checking cache, resource_url={resource_url}, saved_cache_key={saved_cache_key}, cache_keys={list(_azure_upload_cache.keys())}")
        
        # First check if we have a saved cache key from a previous uploader instance
        if saved_cache_key and saved_cache_key in _azure_upload_cache:
            cached_value = _azure_upload_cache[saved_cache_key]
            if cached_value == 'COMPLETE':
                # Already completed - don't try to move again
                self.azure_direct_complete = True
                self.filename = munge.munge_filename(resource_url.rsplit('/', 1)[-1]) if resource_url else None
                log.info(f"ResourceCloudStorage init: Recovered COMPLETE status from cache key {saved_cache_key}")
            else:
                # New resource with temp path
                self.azure_temp_path = cached_value
                path_parts = cached_value.split('/')
                if len(path_parts) >= 1:
                    self.filename = munge.munge_filename(path_parts[-1])
                log.info(f"ResourceCloudStorage init: Recovered azure_temp_path from cache key {saved_cache_key}: {cached_value}")
        
        # Also check if azure_temp_path was saved in the resource dict (backup mechanism)
        saved_azure_temp_path = resource.get('_azure_temp_path')
        if saved_azure_temp_path and self.can_use_advanced_azure and not self.azure_temp_path and not self.azure_direct_complete:
            # Check if this temp path was already moved in a previous request (multi-worker environment)
            if saved_azure_temp_path in _azure_completed_moves:
                # Already completed by another request, mark as complete
                self.azure_direct_complete = True
                path_parts = saved_azure_temp_path.split('/')
                if len(path_parts) >= 1:
                    self.filename = munge.munge_filename(path_parts[-1])
                log.info(f"ResourceCloudStorage init: Temp path {saved_azure_temp_path} already moved (found in completed moves), marking as complete")
            else:
                # Only use this if we haven't already recovered from cache and it's not marked complete
                self.azure_temp_path = saved_azure_temp_path
                # Extract filename from saved path
                path_parts = saved_azure_temp_path.split('/')
                if len(path_parts) >= 1:
                    self.filename = munge.munge_filename(path_parts[-1])
                log.info(f"ResourceCloudStorage init: Recovered azure_temp_path from resource dict: {saved_azure_temp_path}, filename={self.filename}")
        
        # Log what we received (use INFO level to ensure visibility)
        log.info(f"ResourceCloudStorage init: azure_blob_path={azure_blob_path}, azure_upload={azure_upload}, can_use_advanced_azure={self.can_use_advanced_azure}")
        if upload_field_storage:
            log.info(f"ResourceCloudStorage init: upload_field_storage type={type(upload_field_storage)}")

        # Check to see if a file has been provided
        # First check for standard file upload (file actually present with a filename)
        has_standard_upload = (
            isinstance(upload_field_storage, (ALLOWED_UPLOAD_TYPES)) and 
            upload_field_storage.filename and 
            len(upload_field_storage.filename) > 0
        )
        
        if has_standard_upload:
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = _get_underlying_file(upload_field_storage)
            resource['url'] = self.filename
            resource['url_type'] = 'upload'
            resource['last_modified'] = datetime.utcnow()
            log.info(f"Standard upload detected: filename={self.filename}")
        elif azure_blob_path and azure_upload and self.can_use_advanced_azure:
            # Azure Direct Upload: file is already in Azure
            # Path can be temp/{uuid}/{filename} (new resource) or resources/{id}/{filename} (existing resource)
            path_parts = azure_blob_path.split('/')
            log.info(f"Azure Direct Upload: Processing blob path={azure_blob_path}, parts={path_parts}")
            
            if len(path_parts) >= 3 and path_parts[0] == 'temp':
                # New resource: file uploaded to temp path, needs to be moved later
                temp_uuid = path_parts[1]  # Extract the UUID from temp/{uuid}/{filename}
                self.filename = munge.munge_filename(path_parts[-1])
                self.azure_temp_path = azure_blob_path
                resource['url'] = self.filename
                resource['url_type'] = 'upload'
                resource['last_modified'] = datetime.utcnow()
                # Store in module-level cache using temp_uuid as unique key (avoids filename collisions)
                cache_key = f"temp_{temp_uuid}"
                _azure_upload_cache[cache_key] = azure_blob_path
                # Also store in resource dict as backup with unique cache key reference
                resource['_azure_temp_path'] = azure_blob_path
                resource['_azure_cache_key'] = cache_key
                log.info(f"Azure Direct Upload (NEW): temp path={azure_blob_path}, filename={self.filename}, cache_key={cache_key}")
            elif len(path_parts) >= 3 and path_parts[0] == 'resources':
                # Existing resource: file already uploaded to final path, no move needed
                resource_id = path_parts[1]  # Extract the resource ID from resources/{id}/{filename}
                self.filename = munge.munge_filename(path_parts[-1])
                # Mark that upload is already complete - no file_upload means upload() won't try to upload again
                self.azure_direct_complete = True
                resource['url'] = self.filename
                resource['url_type'] = 'upload'
                resource['last_modified'] = datetime.utcnow()
                # Store in cache to mark as complete for later uploader instances, using resource_id as unique key
                cache_key = f"existing_{resource_id}"
                _azure_upload_cache[cache_key] = 'COMPLETE'
                resource['_azure_cache_key'] = cache_key
                log.info(f"Azure Direct Upload (EXISTING): final path={azure_blob_path}, filename={self.filename}, cache_key={cache_key}, no move needed")
            else:
                log.warning(f"Azure Direct Upload: Invalid blob path format: {azure_blob_path}")
        elif azure_blob_path and azure_upload:
            # Azure upload requested but advanced Azure not available
            log.warning(f"Azure Direct Upload requested but can_use_advanced_azure={self.can_use_advanced_azure}")
        elif multipart_name and self.can_use_advanced_aws:
            # This means that file was successfully uploaded and stored
            # at cloud.
            # Currently implemented just AWS version
            resource['url'] = munge.munge_filename(multipart_name)
            resource['url_type'] = 'upload'
        elif self._clear and resource.get('id'):
            # Apparently, this is a created-but-not-commited resource whose
            # file upload has been canceled. We're copying the behaviour of
            # ckaenxt-s3filestore here.
            old_resource = model.Session.query(
                model.Resource
            ).get(
                resource['id']
            )

            self.old_filename = old_resource.url
            resource['url_type'] = ''

    def path_from_filename(self, rid, filename):
        """
        Returns a bucket path for the given resource_id and filename.

        :param rid: The resource ID.
        :param filename: The unmunged resource filename.
        """
        return os.path.join(
            'resources',
            rid,
            munge.munge_filename(filename)
        )

    def get_path(self, resource_id):
        # at this point, any auth should be done already as you
        # have to pass a Resource object to even get the uploader class (canada fork only)
        #TODO: upstream contribution??
        user = get_action('get_site_user')({'ignore_auth': True}, {})
        resource = get_action('resource_show')({"user": user['name']}, {'id': resource_id})
        filename = resource['url'].rsplit('/', 1)[-1]

        return self.get_url_from_filename(resource_id, filename)

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        log.info(f"ResourceCloudStorage.upload() called: id={id}, filename={self.filename}, azure_temp_path={self.azure_temp_path}, azure_direct_complete={getattr(self, 'azure_direct_complete', False)}")
        
        # If Azure Direct Upload to final path is already complete, nothing to do
        if getattr(self, 'azure_direct_complete', False):
            log.info(f"Azure Direct Upload: File already in final location, skipping upload for {self.filename}")
            self._cleanup_azure_cache()
            return 0
        
        if self.filename:
            if self.can_use_advanced_azure:
                return self._upload_azure(id)
            else:
                return self._upload_libcloud(id)

        elif self._clear and self.old_filename and not self.leave_files:
            # This is only set when a previously-uploaded file is replace
            # by a link. We want to delete the previously-uploaded file.
            try:
                self.container.delete_object(
                    self.container.get_object(
                        self.path_from_filename(
                            id,
                            self.old_filename
                        )
                    )
                )
            except ObjectDoesNotExistError:
                # It's possible for the object to have already been deleted, or
                # for it to not yet exist in a committed state due to an
                # outstanding lease.
                return

    def _cleanup_azure_cache(self):
        """Clean up Azure-related cache entries for this resource."""
        saved_cache_key = self.resource.get('_azure_cache_key')
        with _cache_lock:
            if saved_cache_key and saved_cache_key in _azure_upload_cache:
                del _azure_upload_cache[saved_cache_key]
        if '_azure_cache_key' in self.resource:
            del self.resource['_azure_cache_key']
        if '_azure_temp_path' in self.resource:
            del self.resource['_azure_temp_path']

    def _upload_azure(self, id):
        """Handle Azure Blob Storage upload with race condition prevention."""
        from azure.storage.blob import ContentSettings, BlobServiceClient
        
        svc_client = BlobServiceClient.from_connection_string(self.connection_link)
        container_client = svc_client.get_container_client(self.container_name)
        
        # Check if this is an Azure Direct Upload (file already in temp path)
        if self.azure_temp_path:
            return self._move_azure_blob(id, container_client)
        else:
            # Standard upload - file provided via form
            return self._upload_azure_standard(id, container_client)

    def _move_azure_blob(self, id, container_client):
        """
        Move blob from temp path to final path with distributed locking.
        Prevents race conditions in multi-worker environments.
        """
        from azure.storage.blob import ContentSettings, BlobSasPermissions, generate_blob_sas
        import time as time_module
        
        final_path = self.path_from_filename(id, self.filename)
        
        # STEP 1: Check if already processed in this request (prevents duplicate processing)
        if _is_upload_processed_in_request(self.azure_temp_path):
            log.info(f"Azure Direct Upload: Already processed in this request, skipping: {self.azure_temp_path}")
            self._cleanup_azure_cache()
            return 0
        
        # STEP 2: Check memory cache (fast path for same-process duplicates)
        with _cache_lock:
            if self.azure_temp_path in _azure_completed_moves:
                completed_path = _azure_completed_moves[self.azure_temp_path]
                log.info(f"Azure Direct Upload: Blob already moved (memory cache): {self.azure_temp_path} -> {completed_path}")
                _mark_upload_processed_in_request(self.azure_temp_path)
                self._cleanup_azure_cache()
                return 0
        
        # STEP 3: Try to acquire distributed lock (database-backed)
        lock_status, existing_path = _acquire_move_lock(self.azure_temp_path, id, final_path)
        
        if lock_status == 'completed':
            log.info(f"Azure Direct Upload: Already completed (DB): {self.azure_temp_path} -> {existing_path}")
            with _cache_lock:
                _azure_completed_moves[self.azure_temp_path] = existing_path or final_path
            _mark_upload_processed_in_request(self.azure_temp_path)
            self._cleanup_azure_cache()
            return 0
        
        if lock_status == 'already_locked':
            # Another worker is processing this, wait a bit and check destination
            log.info(f"Azure Direct Upload: Another worker is processing, waiting: {self.azure_temp_path}")
            time_module.sleep(1)
            dest_blob = container_client.get_blob_client(final_path)
            try:
                dest_blob.get_blob_properties()
                log.info(f"Azure Direct Upload: Destination exists after wait: {final_path}")
                _mark_move_completed_db(self.azure_temp_path, final_path)
                with _cache_lock:
                    _azure_completed_moves[self.azure_temp_path] = final_path
                _mark_upload_processed_in_request(self.azure_temp_path)
                self._cleanup_azure_cache()
                return 0
            except Exception:
                log.warning(f"Azure Direct Upload: Destination not ready after wait, proceeding anyway")
        
        # STEP 4: We have the lock, perform the actual move
        log.info(f"Azure Direct Upload: Acquired lock, moving blob: {self.azure_temp_path} -> {final_path}")
        
        source_blob = container_client.get_blob_client(self.azure_temp_path)
        dest_blob = container_client.get_blob_client(final_path)
        
        try:
            # First check if destination already exists (another worker may have completed)
            try:
                dest_blob.get_blob_properties()
                log.info(f"Azure Direct Upload: Destination already exists: {final_path}")
                self._finalize_move(self.azure_temp_path, final_path, source_blob)
                return 0
            except Exception:
                pass  # Destination doesn't exist, continue with move
            
            # Verify source blob exists
            try:
                source_blob.get_blob_properties()
            except Exception as e:
                # Source doesn't exist - check if dest exists (race condition)
                try:
                    dest_blob.get_blob_properties()
                    log.info(f"Azure Direct Upload: Source gone but dest exists (race): {final_path}")
                    self._finalize_move(self.azure_temp_path, final_path, None)
                    return 0
                except Exception:
                    log.warning(f"Azure Direct Upload: Neither source nor dest exists: {self.azure_temp_path}")
                    _mark_move_failed_db(self.azure_temp_path)
                    self._cleanup_azure_cache()
                    return 0
            
            # Generate SAS token for copy operation
            permissions = BlobSasPermissions(read=True)
            token_expires = datetime.utcnow() + timedelta(hours=1)
            sas_token = generate_blob_sas(
                account_name=source_blob.account_name,
                account_key=source_blob.credential.account_key,
                container_name=source_blob.container_name,
                blob_name=source_blob.blob_name,
                permission=permissions,
                expiry=token_expires
            )
            source_url_with_sas = f"{source_blob.url}?{sas_token}"
            
            # Perform the copy
            dest_blob.start_copy_from_url(source_url_with_sas)
            
            # Wait for copy to complete
            copy_props = dest_blob.get_blob_properties()
            copy_status = copy_props.copy.status if copy_props.copy else None
            wait_count = 0
            max_wait = 60  # Max 30 seconds
            while copy_status == 'pending' and wait_count < max_wait:
                time_module.sleep(0.5)
                wait_count += 1
                copy_props = dest_blob.get_blob_properties()
                copy_status = copy_props.copy.status if copy_props.copy else 'success'
            
            if copy_status == 'success' or copy_status is None:
                log.info(f"Azure Direct Upload: Blob copied successfully to {final_path}")
                self._finalize_move(self.azure_temp_path, final_path, source_blob)
                
                # Set content type if enabled
                if self.guess_mimetype:
                    content_type, _ = mimetypes.guess_type(self.filename)
                    if not content_type:
                        content_type = 'application/octet-stream'
                    from azure.storage.blob import ContentSettings
                    dest_blob.set_http_headers(ContentSettings(content_type=content_type))
            else:
                log.error(f"Azure Direct Upload: Copy failed with status {copy_status}")
                _mark_move_failed_db(self.azure_temp_path)
            
            return 0
            
        except Exception as e:
            log.error(f"Azure Direct Upload: Error moving blob: {e}")
            _mark_move_failed_db(self.azure_temp_path)
            raise

    def _finalize_move(self, temp_path, final_path, source_blob):
        """Mark move as completed and clean up."""
        # Mark completed in database
        _mark_move_completed_db(temp_path, final_path)
        
        # Update memory cache
        with _cache_lock:
            _azure_completed_moves[temp_path] = final_path
        
        # Mark processed in request
        _mark_upload_processed_in_request(temp_path)
        
        # Delete temp blob if we have access
        if source_blob:
            try:
                source_blob.delete_blob()
                log.info(f"Azure Direct Upload: Deleted temp blob {temp_path}")
            except Exception as e:
                log.debug(f"Azure Direct Upload: Could not delete temp blob: {e}")
        
        # Clean up cache entries
        self._cleanup_azure_cache()

    def _upload_azure_standard(self, id, container_client):
        """Standard Azure upload - file provided via form."""
        from azure.storage.blob import ContentSettings
        
        blob_client = container_client.get_blob_client(
            self.path_from_filename(id, self.filename)
        )
        stream = self.file_upload
        blob_client.upload_blob(stream, overwrite=True)
        
        if self.guess_mimetype:
            content_type, _ = mimetypes.guess_type(self.filename)
            if not content_type:
                content_type = 'application/octet-stream'
            blob_client.set_http_headers(ContentSettings(content_type=content_type))
        
        return stream.tell()

    def _upload_libcloud(self, id):
        """Upload using libcloud (non-Azure providers)."""
        # If it's temporary file, we'd better convert it
        # into FileIO. Otherwise libcloud will iterate
        # over lines, not over chunks
        if isinstance(self.file_upload, SpooledTemporaryFile):
            self.file_upload.rollover()
            try:
                file_upload_iter = self.file_upload._file.detach()
            except AttributeError:
                file_upload_iter = self.file_upload._file
        else:
            file_upload_iter = iter(self.file_upload)

        object_name = self.path_from_filename(id, self.filename)
        self.container.upload_object_via_stream(
            iterator=file_upload_iter,
            object_name=object_name
        )

    def get_url_from_filename(self, rid, filename, content_type=None):
        """
        Retrieve a publically accessible URL for the given resource_id
        and filename.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.
        :param content_type: Optionally a Content-Type header.

        :returns: Externally accessible URL or None.
        """
        def _ensure_https(url):
            if url and url.startswith('http://') and 'blob.core.windows.net' in url:
                return 'https://' + url[len('http://'):]
            return url

        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)
        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
        if self.can_use_advanced_azure and self.use_secure_urls:
            from azure.storage.blob import BlobClient, BlobSasPermissions, BlobServiceClient, generate_blob_sas

            svc_client = BlobServiceClient.from_connection_string(self.connection_link)
            container_client = svc_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(path)
            permissions = BlobSasPermissions(read=True)
            token_expires = datetime.utcnow() + timedelta(hours=1)
            sas_token = generate_blob_sas(account_name=blob_client.account_name,
                                      account_key=blob_client.credential.account_key,
                                      container_name=blob_client.container_name,
                                      blob_name=blob_client.blob_name,
                                      permission=permissions,
                                      expiry=token_expires)

            blob_client = BlobClient(svc_client.url,
                                    container_name=blob_client.container_name,
                                    blob_name=blob_client.blob_name,
                                    credential=sas_token)

            # The url from blob_client above actually generate the url for download
            # but the file path is mixed with the filename e.g
            # we want to download `example.csv` but the url generate this
            # `resource12565-316r3example.csv` which is mung of the filename with its path
            # hence the below method enable the actual download of the file with its name
            url = 'https://{}.blob.core.windows.net/{}/{}?{}'.format(
                blob_client.account_name,
                self.container_name,
                path,
                sas_token
            )

            return _ensure_https(url)

        elif self.can_use_advanced_aws and self.use_secure_urls:

            from boto.s3.connection import S3Connection
            os.environ['S3_USE_SIGV4'] = 'True'
            s3_connection = S3Connection(
                aws_access_key_id=self.driver_options['key'],
                aws_secret_access_key=self.driver_options['secret'],
                security_token=self.driver_options['token'],
                host='s3.eu-west-1.amazonaws.com'
            )

            generate_url_params = {"expires_in": 60 * 60,
                                   "method": "GET",
                                   "bucket": self.container_name,
                                   "key": path}
            if content_type:
                generate_url_params['headers'] = {"Content-Type": content_type}

            return s3_connection.generate_url_sigv4(**generate_url_params)

        # Find the object for the given key. Some drivers (eg Azure via
        # libcloud) raise instead of returning None for missing objects.
        try:
            obj = self.container.get_object(path)
        except ObjectDoesNotExistError:
            return
        if obj is None:
            return

        # Not supported by all providers!
        try:
            return _ensure_https(self.driver.get_object_cdn_url(obj))
        except NotImplementedError:
            if 'S3' in self.driver_name:
                from urllib.parse import urljoin
                return urljoin(
                    'https://' + self.driver.connection.host,
                    '{container}/{path}'.format(
                        container=self.container_name,
                        path=path
                    )
                )
            # This extra 'url' property isn't documented anywhere, sadly.
            # See azure_blobs.py:_xml_to_object for more.
            elif 'url' in obj.extra:
                return _ensure_https(obj.extra['url'])
            raise

    @property
    def package(self):
        return model.Package.get(self.resource['package_id'])
