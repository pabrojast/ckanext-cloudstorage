ckan.module('cloudstorage-azure-direct-upload', function($, _) {
    'use strict';

    return {
        options: {
            i18n: {
                resource_create: _('Resource has been created.'),
                resource_update: _('Resource has been updated.'),
                upload_completed: _('Upload completed. You will be redirected in few seconds...'),
                upload_failed: _('Upload failed. Please try again.'),
                generating_url: _('Preparing upload...'),
                uploading: _('Uploading file...')
            }
        },

        _packageId: null,
        _resourceId: null,
        _uploadUrl: null,
        _blobPath: null,
        _clickedBtn: null,
        _redirect_url: null,

        initialize: function() {
            $.proxyAll(this, /_on/);
            this.options.packageId = this.options.packageId.slice(1);
            this._form = this.$('form');
            // Support both default field IDs and custom ones passed as options
            var uploadFieldId = this.options.fieldUpload || 'field-image-upload';
            var urlFieldId = this.options.fieldUrl || 'field-image-url';
            this._file = $('#' + uploadFieldId);
            this._url = $('#' + urlFieldId);
            this._save = $('[name=save]');
            this._id = $('input[name=id]');
            
            // Create progress bar
            this._progress = $('<div>', {
                class: 'progress hidden'
            });
            this._bar = $('<div>', {
                class: 'progress-bar progress-bar-striped active'
            });
            this._progress.append(this._bar);
            this._progress.insertAfter(this._url.parent().parent());

            this._save.on('click', this._onSaveClick);
            this._file.on('change', this._onFileSelected);
        },

        _onFileSelected: function(event) {
            var file = event.target.files[0];
            if (file) {
                this._setProgress(0, this._bar);
                this._progress.removeClass('hidden').show('slow');
                this.sandbox.notify(
                    'File selected',
                    'File: ' + file.name + ', Size: ' + this._formatFileSize(file.size),
                    'info'
                );
            }
        },

        _onSaveClick: function(event, pass) {
            if (pass || !window.FileList || !this._file || !this._file.val()) {
                return;
            }
            event.preventDefault();

            var file = this._file[0].files[0];
            if (!file) {
                this.sandbox.notify('Error', 'Please select a file to upload', 'error');
                return;
            }

            this._onDisableSave(true);
            this._clickedBtn = event.target.value;
            this._onSaveForm(file);
        },

        _onSaveForm: function(file) {
            var self = this;
            var formData = this._form.serializeArray().reduce(
                function (result, item) {
                    result[item.name] = item.value;
                    return result;
                }, {});

            formData.url = file.name;
            formData.package_id = this.options.packageId;
            formData.size = file.size;
            formData.url_type = 'upload';
            
            var action = formData.id ? 'resource_update' : 'resource_create';
            
            this.sandbox.client.call(
                'POST',
                action,
                formData,
                function (data) {
                    var result = data.result;
                    self._packageId = result.package_id;
                    self._resourceId = result.id;
                    self._id.val(result.id);
                    
                    self.sandbox.notify(
                        'Resource saved',
                        self.i18n(action, {id: result.id}),
                        'success'
                    );
                    
                    // Now proceed with Azure direct upload
                    self._onPerformDirectUpload(file);
                },
                function (err, st, msg) {
                    self.sandbox.notify('Error', msg, 'error');
                    self._onHandleError('Unable to save resource');
                }
            );
        },

        _onPerformDirectUpload: function(file) {
            var self = this;
            
            this.sandbox.notify(
                'Preparing upload',
                this.i18n('generating_url'),
                'info'
            );
            
            // Generate Azure direct upload URL
            this.sandbox.client.call(
                'POST',
                'cloudstorage_generate_azure_direct_upload_url',
                {
                    resource_id: this._resourceId,
                    filename: file.name,
                    file_size: file.size
                },
                function (data) {
                    var result = data.result;
                    self._uploadUrl = result.upload_url;
                    self._blobPath = result.blob_path;
                    
                    // Perform the direct upload to Azure
                    self._onUploadToAzure(file, result);
                },
                function (err, st, msg) {
                    self.sandbox.notify('Error', 'Failed to generate upload URL: ' + msg, 'error');
                    self._onHandleError('Unable to generate upload URL');
                }
            );
        },

        _onUploadToAzure: function(file, uploadInfo) {
            var self = this;
            
            this.sandbox.notify(
                'Uploading',
                this.i18n('uploading'),
                'info'
            );
            
            // Create XMLHttpRequest for direct Azure upload
            var xhr = new XMLHttpRequest();
            
            // Track upload progress
            xhr.upload.addEventListener('progress', function(e) {
                if (e.lengthComputable) {
                    var progress = (e.loaded / e.total) * 100;
                    self._setProgress(progress, self._bar);
                }
            });
            
            xhr.addEventListener('load', function() {
                if (xhr.status === 201) {
                    // Upload successful, confirm with CKAN
                    self._onConfirmUpload();
                } else {
                    self.sandbox.notify(
                        'Upload failed', 
                        'Azure returned status: ' + xhr.status + ' ' + xhr.statusText, 
                        'error'
                    );
                    self._onHandleError('Upload to Azure failed');
                }
            });
            
            xhr.addEventListener('error', function() {
                self.sandbox.notify('Upload failed', 'Network error during upload', 'error');
                self._onHandleError('Upload to Azure failed');
            });
            
            // Prepare the request
            xhr.open('PUT', uploadInfo.upload_url, true);
            
            // Set required headers
            for (var header in uploadInfo.headers) {
                if (uploadInfo.headers.hasOwnProperty(header)) {
                    xhr.setRequestHeader(header, uploadInfo.headers[header]);
                }
            }
            
            // Send the file
            xhr.send(file);
        },

        _onConfirmUpload: function() {
            var self = this;
            
            // Confirm upload with CKAN
            this.sandbox.client.call(
                'POST',
                'cloudstorage_confirm_azure_direct_upload',
                {
                    resource_id: this._resourceId,
                    filename: this._file[0].files[0].name,
                    blob_path: this._blobPath
                },
                function (data) {
                    self._progress.hide('fast');
                    self._onDisableSave(false);
                    
                    self.sandbox.notify(
                        'Success',
                        self.i18n('upload_completed'),
                        'success'
                    );
                    
                    // Redirect based on which button was clicked
                    if (self._resourceId && self._packageId) {
                        var path;
                        if (self._clickedBtn == 'again') {
                            path = '/dataset/new_resource/';
                        } else if (self._clickedBtn == 'go-dataset') {
                            path = '/dataset/edit/';
                        } else {
                            path = '/dataset/';
                        }
                        
                        var redirect_url = self.sandbox.url(path + self._packageId);
                        self._form.attr('action', redirect_url);
                        self._form.attr('method', 'GET');
                        self.$('[name]').attr('name', null);
                        
                        setTimeout(function(){
                            self._form.submit();
                        }, 3000);
                    }
                },
                function (err, st, msg) {
                    self.sandbox.notify('Error', 'Failed to confirm upload: ' + msg, 'error');
                    self._onHandleError('Failed to confirm upload');
                }
            );
            
            this._setProgressType('success', this._progress);
        },

        _onDisableSave: function (value) {
            this._save.attr('disabled', value);
        },

        _setProgress: function (progress, bar) {
            bar.css('width', progress + '%').text(Math.round(progress) + '%');
        },

        _setProgressType: function (type, progress) {
            progress
                .removeClass('progress-success progress-danger progress-info')
                .addClass('progress-' + type);
        },

        _onHandleError: function (msg) {
            this.sandbox.notify('Error', msg, 'error');
            this._onDisableSave(false);
            this._setProgressType('danger', this._progress);
        },

        _formatFileSize: function(bytes) {
            if (bytes === 0) return '0 Bytes';
            var k = 1024;
            var sizes = ['Bytes', 'KB', 'MB', 'GB'];
            var i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
    };
});