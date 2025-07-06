/**
 * Enhanced Cloud Storage Upload Module
 * 
 * Features:
 * - Multiple file selection and upload
 * - Drag and drop support
 * - Upload queue management
 * - Progress tracking per file and overall
 * - Azure direct upload support
 * - Fallback to multipart upload
 */
ckan.module('cloudstorage-enhanced-upload', function($, _) {
    'use strict';

    return {
        options: {
            i18n: {
                upload_completed: _('Upload completed'),
                upload_failed: _('Upload failed'),
                file_too_large: _('File is too large. Maximum size is 5GB.'),
                invalid_file_type: _('Invalid file type'),
                uploading: _('Uploading...'),
                pending: _('Pending'),
                success: _('Success'),
                error: _('Error'),
                generating_url: _('Preparing upload...'),
                creating_resource: _('Creating resource...')
            },
            maxFileSize: 5 * 1024 * 1024 * 1024, // 5GB
            allowedExtensions: null, // null = all extensions allowed
            chunkSize: 5 * 1024 * 1024 // 5MB chunks for multipart
        },

        // Internal state
        _fileQueue: [],
        _uploadInProgress: false,
        _currentFileId: 0,
        _completedFiles: 0,
        _totalFiles: 0,
        _packageId: null,
        _useAzureDirect: false,
        _useMultipart: false,

        initialize: function() {
            $.proxyAll(this, /_on/);
            
            // Get configuration
            this._packageId = this.options.packageId;
            this._useAzureDirect = this.options.azureDirect === 'true';
            this._useMultipart = this.options.multipart === 'true';
            
            // Cache DOM elements
            this._dropzone = this.$('#upload-dropzone');
            this._fileInput = this.$('#field-upload-multiple');
            this._browseBtn = this.$('.btn-browse-files');
            this._uploadAllBtn = this.$('.btn-upload-all');
            this._clearQueueBtn = this.$('.btn-clear-queue');
            this._queueContainer = this.$('#upload-queue');
            this._queueList = this.$('#upload-queue-list');
            this._overallProgress = this.$('#overall-progress');
            this._overallProgressBar = this.$('#overall-progress-bar');
            this._filesCompleted = this.$('#files-completed');
            this._filesTotal = this.$('#files-total');
            this._urlInput = this.$('#field-url');
            
            // Template for queue items
            this._itemTemplate = _.template($('#upload-queue-item-template').html());
            
            // Bind events
            this._setupEventHandlers();
        },

        _setupEventHandlers: function() {
            // File selection
            this._browseBtn.on('click', this._onBrowseClick);
            this._fileInput.on('change', this._onFileSelect);
            
            // Drag and drop
            this._dropzone.on('click', this._onDropzoneClick);
            this._dropzone.on('dragover', this._onDragOver);
            this._dropzone.on('dragleave', this._onDragLeave);
            this._dropzone.on('drop', this._onDrop);
            
            // Queue actions
            this._uploadAllBtn.on('click', this._onUploadAll);
            this._clearQueueBtn.on('click', this._onClearQueue);
            
            // Delegated events for queue items
            this._queueList.on('click', '.btn-upload-file', this._onUploadFile);
            this._queueList.on('click', '.btn-remove-file', this._onRemoveFile);
            
            // Prevent default drag behavior on document
            $(document).on('dragover drop', function(e) {
                e.preventDefault();
            });
        },

        _onBrowseClick: function(e) {
            e.preventDefault();
            this._fileInput.click();
        },

        _onDropzoneClick: function(e) {
            if (e.target === this._dropzone[0] || $(e.target).closest('.dropzone-content').length) {
                this._fileInput.click();
            }
        },

        _onDragOver: function(e) {
            e.preventDefault();
            e.stopPropagation();
            this._dropzone.addClass('dragover');
        },

        _onDragLeave: function(e) {
            e.preventDefault();
            e.stopPropagation();
            this._dropzone.removeClass('dragover');
        },

        _onDrop: function(e) {
            e.preventDefault();
            e.stopPropagation();
            this._dropzone.removeClass('dragover');
            
            var files = e.originalEvent.dataTransfer.files;
            this._addFilesToQueue(files);
        },

        _onFileSelect: function(e) {
            var files = e.target.files;
            this._addFilesToQueue(files);
            // Reset input to allow selecting same files again
            this._fileInput.val('');
        },

        _addFilesToQueue: function(files) {
            var validFiles = [];
            
            for (var i = 0; i < files.length; i++) {
                var file = files[i];
                
                // Validate file size
                if (file.size > this.options.maxFileSize) {
                    this.notify(this.options.i18n.file_too_large + ' (' + file.name + ')', 'error');
                    continue;
                }
                
                // Validate file extension if restrictions are set
                if (this.options.allowedExtensions) {
                    var ext = file.name.split('.').pop().toLowerCase();
                    if (this.options.allowedExtensions.indexOf(ext) === -1) {
                        this.notify(this.options.i18n.invalid_file_type + ' (' + file.name + ')', 'error');
                        continue;
                    }
                }
                
                validFiles.push(file);
            }
            
            // Add valid files to queue
            for (var j = 0; j < validFiles.length; j++) {
                this._addFileToQueue(validFiles[j]);
            }
            
            // Update UI
            if (this._fileQueue.length > 0) {
                this._queueContainer.removeClass('hidden');
                this._updateTotalFiles();
            }
        },

        _addFileToQueue: function(file) {
            var fileId = 'file-' + (++this._currentFileId);
            var fileItem = {
                id: fileId,
                file: file,
                status: 'pending',
                progress: 0,
                resourceId: null
            };
            
            this._fileQueue.push(fileItem);
            
            // Render queue item
            var html = this._itemTemplate({
                fileId: fileId,
                fileName: file.name,
                fileSize: this._formatFileSize(file.size)
            });
            
            this._queueList.append(html);
        },

        _formatFileSize: function(bytes) {
            if (bytes === 0) return '0 Bytes';
            var k = 1024;
            var sizes = ['Bytes', 'KB', 'MB', 'GB'];
            var i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        },

        _onUploadAll: function(e) {
            e.preventDefault();
            if (this._uploadInProgress) return;
            
            // Disable URL input when uploading files
            this._urlInput.prop('disabled', true);
            
            this._uploadInProgress = true;
            this._overallProgress.removeClass('hidden');
            this._uploadNextFile();
        },

        _uploadNextFile: function() {
            // Find next pending file
            var nextFile = null;
            for (var i = 0; i < this._fileQueue.length; i++) {
                if (this._fileQueue[i].status === 'pending') {
                    nextFile = this._fileQueue[i];
                    break;
                }
            }
            
            if (!nextFile) {
                // All files processed
                this._uploadInProgress = false;
                this._urlInput.prop('disabled', false);
                this._updateOverallProgress();
                
                if (this._completedFiles === this._totalFiles) {
                    this.notify(this.options.i18n.upload_completed, 'success');
                }
                return;
            }
            
            // Upload the file
            this._uploadFile(nextFile);
        },

        _onUploadFile: function(e) {
            e.preventDefault();
            var fileId = $(e.currentTarget).data('file-id');
            var fileItem = this._getFileById(fileId);
            
            if (fileItem && fileItem.status === 'pending') {
                this._uploadInProgress = true;
                this._overallProgress.removeClass('hidden');
                this._uploadFile(fileItem);
            }
        },

        _uploadFile: function(fileItem) {
            var self = this;
            
            // Update status
            this._updateFileStatus(fileItem.id, 'uploading', this.options.i18n.creating_resource);
            
            // First, create a resource
            this._createResource(fileItem.file.name)
                .done(function(resource) {
                    fileItem.resourceId = resource.id;
                    
                    // Choose upload method
                    if (self._useAzureDirect) {
                        self._uploadAzureDirect(fileItem);
                    } else if (self._useMultipart) {
                        self._uploadMultipart(fileItem);
                    } else {
                        // Fallback to standard form upload
                        self._uploadStandard(fileItem);
                    }
                })
                .fail(function(xhr) {
                    self._handleUploadError(fileItem, xhr);
                });
        },

        _createResource: function(filename) {
            var data = {
                package_id: this._packageId,
                name: filename,
                url: 'upload',  // Placeholder
                format: filename.split('.').pop().toLowerCase()
            };
            
            return $.ajax({
                url: '/api/3/action/resource_create',
                type: 'POST',
                dataType: 'json',
                data: JSON.stringify(data),
                contentType: 'application/json',
                headers: {
                    'X-CKAN-API-Key': this._getApiKey()
                }
            }).then(function(response) {
                return response.result;
            });
        },

        _uploadAzureDirect: function(fileItem) {
            var self = this;
            
            this._updateFileStatus(fileItem.id, 'uploading', this.options.i18n.generating_url);
            
            // Get Azure upload URL
            $.ajax({
                url: '/api/3/action/cloudstorage_generate_azure_direct_upload_url',
                type: 'POST',
                dataType: 'json',
                data: JSON.stringify({
                    resource_id: fileItem.resourceId,
                    filename: fileItem.file.name
                }),
                contentType: 'application/json',
                headers: {
                    'X-CKAN-API-Key': this._getApiKey()
                }
            })
            .done(function(response) {
                var uploadUrl = response.result.upload_url;
                var blobPath = response.result.blob_path;
                
                self._updateFileStatus(fileItem.id, 'uploading', self.options.i18n.uploading);
                
                // Upload directly to Azure
                self._uploadToAzure(fileItem, uploadUrl, blobPath);
            })
            .fail(function(xhr) {
                self._handleUploadError(fileItem, xhr);
            });
        },

        _uploadToAzure: function(fileItem, uploadUrl, blobPath) {
            var self = this;
            var xhr = new XMLHttpRequest();
            
            xhr.upload.addEventListener('progress', function(e) {
                if (e.lengthComputable) {
                    var progress = Math.round((e.loaded / e.total) * 100);
                    self._updateFileProgress(fileItem.id, progress);
                }
            });
            
            xhr.addEventListener('load', function() {
                if (xhr.status === 201) {
                    // Confirm upload with CKAN
                    self._confirmAzureUpload(fileItem, blobPath);
                } else {
                    self._handleUploadError(fileItem, xhr);
                }
            });
            
            xhr.addEventListener('error', function() {
                self._handleUploadError(fileItem, xhr);
            });
            
            xhr.open('PUT', uploadUrl);
            xhr.setRequestHeader('x-ms-blob-type', 'BlockBlob');
            xhr.setRequestHeader('Content-Type', fileItem.file.type || 'application/octet-stream');
            xhr.send(fileItem.file);
        },

        _confirmAzureUpload: function(fileItem, blobPath) {
            var self = this;
            
            $.ajax({
                url: '/api/3/action/cloudstorage_confirm_azure_direct_upload',
                type: 'POST',
                dataType: 'json',
                data: JSON.stringify({
                    resource_id: fileItem.resourceId,
                    blob_path: blobPath,
                    filename: fileItem.file.name,
                    size: fileItem.file.size,
                    mimetype: fileItem.file.type || 'application/octet-stream'
                }),
                contentType: 'application/json',
                headers: {
                    'X-CKAN-API-Key': this._getApiKey()
                }
            })
            .done(function() {
                self._handleUploadSuccess(fileItem);
            })
            .fail(function(xhr) {
                self._handleUploadError(fileItem, xhr);
            });
        },

        _uploadMultipart: function(fileItem) {
            // Implementation for multipart upload
            // This would be similar to the existing cloudstorage-multipart-upload.js
            // but adapted for multiple files
            this.notify('Multipart upload not yet implemented for multiple files', 'warning');
            this._handleUploadError(fileItem, {responseText: 'Not implemented'});
        },

        _uploadStandard: function(fileItem) {
            // Fallback standard upload using FormData
            var self = this;
            var formData = new FormData();
            
            formData.append('id', fileItem.resourceId);
            formData.append('upload', fileItem.file);
            
            $.ajax({
                url: '/api/3/action/resource_update',
                type: 'POST',
                data: formData,
                processData: false,
                contentType: false,
                headers: {
                    'X-CKAN-API-Key': this._getApiKey()
                },
                xhr: function() {
                    var xhr = new window.XMLHttpRequest();
                    xhr.upload.addEventListener('progress', function(e) {
                        if (e.lengthComputable) {
                            var progress = Math.round((e.loaded / e.total) * 100);
                            self._updateFileProgress(fileItem.id, progress);
                        }
                    });
                    return xhr;
                }
            })
            .done(function() {
                self._handleUploadSuccess(fileItem);
            })
            .fail(function(xhr) {
                self._handleUploadError(fileItem, xhr);
            });
        },

        _handleUploadSuccess: function(fileItem) {
            fileItem.status = 'success';
            this._completedFiles++;
            this._updateFileStatus(fileItem.id, 'success', this.options.i18n.success);
            this._updateOverallProgress();
            
            // Continue with next file
            this._uploadNextFile();
        },

        _handleUploadError: function(fileItem, xhr) {
            fileItem.status = 'error';
            this._completedFiles++;
            
            var errorMsg = this.options.i18n.error;
            if (xhr && xhr.responseJSON && xhr.responseJSON.error) {
                errorMsg += ': ' + xhr.responseJSON.error.message;
            }
            
            this._updateFileStatus(fileItem.id, 'error', errorMsg);
            this._updateOverallProgress();
            
            // Continue with next file
            this._uploadNextFile();
        },

        _updateFileStatus: function(fileId, status, message) {
            var $item = this._queueList.find('[data-file-id="' + fileId + '"]');
            var $status = $item.find('.status-text');
            var $actions = $item.find('.file-actions');
            var $progress = $item.find('.file-progress');
            
            $status.removeClass('text-muted status-uploading status-success status-error');
            
            switch(status) {
                case 'uploading':
                    $status.addClass('status-uploading').text(message);
                    $actions.addClass('hidden');
                    $progress.removeClass('hidden');
                    break;
                case 'success':
                    $status.addClass('status-success').html('<i class="fa fa-check"></i> ' + message);
                    $progress.addClass('hidden');
                    break;
                case 'error':
                    $status.addClass('status-error').html('<i class="fa fa-times"></i> ' + message);
                    $progress.addClass('hidden');
                    break;
            }
        },

        _updateFileProgress: function(fileId, progress) {
            var $item = this._queueList.find('[data-file-id="' + fileId + '"]');
            var $progressBar = $item.find('.progress-bar');
            var $progressText = $item.find('.progress-text');
            
            $progressBar.css('width', progress + '%');
            $progressText.text(progress + '%');
            
            // Update file item progress
            var fileItem = this._getFileById(fileId);
            if (fileItem) {
                fileItem.progress = progress;
            }
        },

        _updateOverallProgress: function() {
            var totalProgress = 0;
            for (var i = 0; i < this._fileQueue.length; i++) {
                if (this._fileQueue[i].status === 'success') {
                    totalProgress += 100;
                } else if (this._fileQueue[i].status === 'error') {
                    totalProgress += 100;
                } else {
                    totalProgress += this._fileQueue[i].progress;
                }
            }
            
            var overallProgress = this._totalFiles > 0 ? Math.round(totalProgress / this._totalFiles) : 0;
            
            this._overallProgressBar.css('width', overallProgress + '%');
            this._overallProgressBar.find('.progress-text').text(overallProgress + '%');
            this._filesCompleted.text(this._completedFiles);
        },

        _updateTotalFiles: function() {
            this._totalFiles = this._fileQueue.length;
            this._filesTotal.text(this._totalFiles);
        },

        _onRemoveFile: function(e) {
            e.preventDefault();
            var fileId = $(e.currentTarget).data('file-id');
            
            // Remove from queue
            this._fileQueue = this._fileQueue.filter(function(item) {
                return item.id !== fileId;
            });
            
            // Remove from UI
            this._queueList.find('[data-file-id="' + fileId + '"]').remove();
            
            // Update counts
            this._updateTotalFiles();
            
            // Hide queue if empty
            if (this._fileQueue.length === 0) {
                this._queueContainer.addClass('hidden');
                this._overallProgress.addClass('hidden');
            }
        },

        _onClearQueue: function(e) {
            e.preventDefault();
            
            // Only clear pending files
            this._fileQueue = this._fileQueue.filter(function(item) {
                return item.status !== 'pending';
            });
            
            // Remove pending items from UI
            this._queueList.find('.queue-item').each(function() {
                var $item = $(this);
                if ($item.find('.status-text').hasClass('text-muted')) {
                    $item.remove();
                }
            });
            
            // Update UI
            this._updateTotalFiles();
            
            if (this._fileQueue.length === 0) {
                this._queueContainer.addClass('hidden');
                this._overallProgress.addClass('hidden');
            }
        },

        _getFileById: function(fileId) {
            for (var i = 0; i < this._fileQueue.length; i++) {
                if (this._fileQueue[i].id === fileId) {
                    return this._fileQueue[i];
                }
            }
            return null;
        },

        _getApiKey: function() {
            // Try to get API key from various sources
            return typeof window.apikey !== 'undefined' ? window.apikey : '';
        },

        notify: function(message, type) {
            // Use CKAN's notification system if available
            if (typeof ckan.notify !== 'undefined') {
                ckan.notify(message, type);
            } else {
                console.log('[' + type + '] ' + message);
            }
        }
    };
});