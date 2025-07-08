/**
 * CloudStorage Resource Editor
 * Complete interface for uploading and managing resources with cloud storage
 */

(function() {
    'use strict';

    // CloudStorage Resource Editor Class
    function CloudStorageResourceEditor(containerId, options) {
        this.container = document.getElementById(containerId);
        this.options = Object.assign({
            packageId: '',
            resourceId: '',
            apiToken: '',
            baseUrl: '',
            uploadType: 'multipart', // 'multipart' or 'azure_direct'
        }, options || {});
        
        this.currentFile = null;
        this.uploadInProgress = false;
        
        this.init();
    }

    CloudStorageResourceEditor.prototype = {
        init: function() {
            if (!this.container) {
                console.error('CloudStorage Resource Editor: Container not found');
                return;
            }
            
            this.render();
            this.bindEvents();
        },

        render: function() {
            this.container.innerHTML = this.getTemplate();
            this.cacheElements();
        },

        cacheElements: function() {
            this.uploadArea = this.container.querySelector('.cloudstorage-upload-area');
            this.fileInput = this.container.querySelector('.cloudstorage-file-input');
            this.progressContainer = this.container.querySelector('.cloudstorage-progress');
            this.progressBar = this.container.querySelector('.cloudstorage-progress-bar');
            this.progressText = this.container.querySelector('.cloudstorage-progress-text');
            this.fileInfo = this.container.querySelector('.cloudstorage-file-info');
            this.form = this.container.querySelector('.cloudstorage-resource-form');
            this.saveButton = this.container.querySelector('.cloudstorage-btn-save');
            this.alertContainer = this.container.querySelector('.cloudstorage-alerts');
        },

        getTemplate: function() {
            return `
                <div class="cloudstorage-resource-editor">
                    <h2>
                        <i class="fa fa-cloud-upload"></i>
                        ${this.options.resourceId ? 'Editar Recurso' : 'Añadir Recurso'}
                    </h2>
                    
                    <div class="cloudstorage-alerts"></div>
                    
                    <div class="cloudstorage-upload-area">
                        <div class="cloudstorage-upload-icon">
                            <i class="fa fa-cloud-upload"></i>
                        </div>
                        <div class="cloudstorage-upload-text">
                            Arrastra un archivo aquí o haz clic para seleccionar
                        </div>
                        <div class="cloudstorage-upload-subtext">
                            Formatos soportados: CSV, JSON, XML, PDF, XLS, XLSX y más
                        </div>
                        <input type="file" class="cloudstorage-file-input" accept="*/*">
                    </div>
                    
                    <div class="cloudstorage-progress">
                        <div class="cloudstorage-progress-bar">
                            <div class="cloudstorage-progress-text">0%</div>
                        </div>
                    </div>
                    
                    <div class="cloudstorage-file-info">
                        <h4>Archivo Seleccionado</h4>
                        <div class="cloudstorage-file-details">
                            <strong>Nombre:</strong>
                            <span class="file-name"></span>
                            <strong>Tamaño:</strong>
                            <span class="file-size"></span>
                            <strong>Tipo:</strong>
                            <span class="file-type"></span>
                        </div>
                    </div>
                    
                    <form class="cloudstorage-resource-form">
                        <div class="cloudstorage-row">
                            <div class="cloudstorage-col">
                                <div class="cloudstorage-form-group">
                                    <label for="resource-name">Nombre del Recurso *</label>
                                    <input type="text" id="resource-name" name="name" required>
                                </div>
                            </div>
                            <div class="cloudstorage-col">
                                <div class="cloudstorage-form-group">
                                    <label for="resource-format">Formato</label>
                                    <input type="text" id="resource-format" name="format">
                                </div>
                            </div>
                        </div>
                        
                        <div class="cloudstorage-form-group">
                            <label for="resource-description">Descripción</label>
                            <textarea id="resource-description" name="description" rows="3"></textarea>
                        </div>
                        
                        <div class="cloudstorage-row">
                            <div class="cloudstorage-col">
                                <div class="cloudstorage-form-group">
                                    <label for="resource-url">URL (opcional)</label>
                                    <input type="url" id="resource-url" name="url">
                                </div>
                            </div>
                            <div class="cloudstorage-col">
                                <div class="cloudstorage-form-group">
                                    <label for="resource-mimetype">Tipo MIME</label>
                                    <input type="text" id="resource-mimetype" name="mimetype">
                                </div>
                            </div>
                        </div>
                        
                        <input type="hidden" name="package_id" value="${this.options.packageId}">
                        <input type="hidden" name="url_type" value="upload">
                        <input type="hidden" name="upload_url" value="">
                        <input type="hidden" name="clear_upload" value="">
                    </form>
                    
                    <div class="cloudstorage-buttons">
                        <a href="${this.getPackageUrl()}" class="cloudstorage-btn cloudstorage-btn-secondary">
                            Cancelar
                        </a>
                        <button type="button" class="cloudstorage-btn cloudstorage-btn-primary cloudstorage-btn-save" disabled>
                            <span class="save-text">${this.options.resourceId ? 'Actualizar Recurso' : 'Guardar Recurso'}</span>
                            <span class="save-loading cloudstorage-hidden">
                                <span class="cloudstorage-loading"></span> Guardando...
                            </span>
                        </button>
                    </div>
                </div>
            `;
        },

        bindEvents: function() {
            // File upload events
            this.uploadArea.addEventListener('click', () => this.fileInput.click());
            this.uploadArea.addEventListener('dragover', this.handleDragOver.bind(this));
            this.uploadArea.addEventListener('dragleave', this.handleDragLeave.bind(this));
            this.uploadArea.addEventListener('drop', this.handleDrop.bind(this));
            this.fileInput.addEventListener('change', this.handleFileSelect.bind(this));
            
            // Form events
            this.saveButton.addEventListener('click', this.handleSave.bind(this));
            
            // Auto-fill format based on file extension
            this.container.querySelector('#resource-name').addEventListener('blur', this.autoFillFormat.bind(this));
        },

        handleDragOver: function(e) {
            e.preventDefault();
            this.uploadArea.classList.add('dragover');
        },

        handleDragLeave: function(e) {
            e.preventDefault();
            this.uploadArea.classList.remove('dragover');
        },

        handleDrop: function(e) {
            e.preventDefault();
            this.uploadArea.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                this.selectFile(files[0]);
            }
        },

        handleFileSelect: function(e) {
            const files = e.target.files;
            if (files.length > 0) {
                this.selectFile(files[0]);
            }
        },

        selectFile: function(file) {
            this.currentFile = file;
            this.showFileInfo(file);
            this.enableSaveButton();
            
            // Auto-fill form fields
            if (!this.container.querySelector('#resource-name').value) {
                this.container.querySelector('#resource-name').value = file.name;
            }
            this.autoFillFormat();
            this.container.querySelector('#resource-mimetype').value = file.type || '';
        },

        showFileInfo: function(file) {
            this.fileInfo.querySelector('.file-name').textContent = file.name;
            this.fileInfo.querySelector('.file-size').textContent = this.formatFileSize(file.size);
            this.fileInfo.querySelector('.file-type').textContent = file.type || 'Desconocido';
            this.fileInfo.style.display = 'block';
        },

        enableSaveButton: function() {
            this.saveButton.disabled = false;
        },

        autoFillFormat: function() {
            const name = this.container.querySelector('#resource-name').value;
            const formatField = this.container.querySelector('#resource-format');
            
            if (name && !formatField.value) {
                const extension = name.split('.').pop().toUpperCase();
                formatField.value = extension;
            }
        },

        handleSave: function() {
            if (this.uploadInProgress) return;
            
            const formData = this.getFormData();
            if (!this.validateForm(formData)) {
                return;
            }
            
            this.uploadInProgress = true;
            this.showSaveLoading();
            
            if (this.currentFile) {
                this.uploadFile().then(() => {
                    this.saveResource(formData);
                }).catch((error) => {
                    this.handleError('Error al subir el archivo: ' + error.message);
                    this.hideSaveLoading();
                    this.uploadInProgress = false;
                });
            } else {
                this.saveResource(formData);
            }
        },

        getFormData: function() {
            const form = this.form;
            const data = {};
            
            const inputs = form.querySelectorAll('input, textarea, select');
            inputs.forEach(input => {
                data[input.name] = input.value;
            });
            
            return data;
        },

        validateForm: function(data) {
            if (!data.name || data.name.trim() === '') {
                this.showAlert('El nombre del recurso es obligatorio', 'error');
                return false;
            }
            
            if (!this.currentFile && !data.url) {
                this.showAlert('Debe seleccionar un archivo o proporcionar una URL', 'error');
                return false;
            }
            
            return true;
        },

        uploadFile: function() {
            return new Promise((resolve, reject) => {
                if (this.options.uploadType === 'azure_direct') {
                    this.uploadAzureDirect().then(resolve).catch(reject);
                } else {
                    this.uploadMultipart().then(resolve).catch(reject);
                }
            });
        },

        uploadAzureDirect: function() {
            // Azure Direct Upload implementation
            return new Promise((resolve, reject) => {
                this.showProgress(0);
                
                // Call Azure Direct Upload API
                fetch('/api/action/cloudstorage_generate_azure_direct_upload_url', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': this.options.apiToken
                    },
                    body: JSON.stringify({
                        filename: this.currentFile.name,
                        package_id: this.options.packageId
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        const uploadUrl = data.result.upload_url;
                        const finalUrl = data.result.url;
                        
                        // Upload file to Azure
                        return fetch(uploadUrl, {
                            method: 'PUT',
                            body: this.currentFile,
                            headers: {
                                'Content-Type': this.currentFile.type
                            }
                        }).then(response => {
                            if (response.ok) {
                                this.showProgress(100);
                                this.container.querySelector('input[name="upload_url"]').value = finalUrl;
                                resolve();
                            } else {
                                throw new Error('Error al subir a Azure: ' + response.statusText);
                            }
                        });
                    } else {
                        throw new Error(data.error.message || 'Error al generar URL de subida');
                    }
                })
                .catch(reject);
            });
        },

        uploadMultipart: function() {
            // Multipart Upload implementation (S3)
            return new Promise((resolve, reject) => {
                this.showProgress(0);
                
                // Initiate multipart upload
                fetch('/api/action/cloudstorage_initiate_multipart', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': this.options.apiToken
                    },
                    body: JSON.stringify({
                        filename: this.currentFile.name,
                        package_id: this.options.packageId
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        const uploadId = data.result.upload_id;
                        const uploadUrl = data.result.upload_url;
                        
                        // Upload file
                        const formData = new FormData();
                        formData.append('file', this.currentFile);
                        
                        const xhr = new XMLHttpRequest();
                        xhr.upload.addEventListener('progress', (e) => {
                            if (e.lengthComputable) {
                                const percent = Math.round((e.loaded / e.total) * 100);
                                this.showProgress(percent);
                            }
                        });
                        
                        xhr.onload = () => {
                            if (xhr.status === 200) {
                                // Finish multipart upload
                                fetch('/api/action/cloudstorage_finish_multipart', {
                                    method: 'POST',
                                    headers: {
                                        'Content-Type': 'application/json',
                                        'Authorization': this.options.apiToken
                                    },
                                    body: JSON.stringify({
                                        upload_id: uploadId,
                                        filename: this.currentFile.name
                                    })
                                })
                                .then(response => response.json())
                                .then(finishData => {
                                    if (finishData.success) {
                                        this.container.querySelector('input[name="upload_url"]').value = finishData.result.url;
                                        resolve();
                                    } else {
                                        reject(new Error(finishData.error.message || 'Error al finalizar subida'));
                                    }
                                })
                                .catch(reject);
                            } else {
                                reject(new Error('Error al subir archivo: ' + xhr.statusText));
                            }
                        };
                        
                        xhr.onerror = () => reject(new Error('Error de red durante la subida'));
                        xhr.open('POST', uploadUrl);
                        xhr.send(formData);
                    } else {
                        reject(new Error(data.error.message || 'Error al iniciar subida'));
                    }
                })
                .catch(reject);
            });
        },

        saveResource: function(formData) {
            const url = this.options.resourceId ? 
                '/api/action/resource_update' : 
                '/api/action/resource_create';
                
            if (this.options.resourceId) {
                formData.id = this.options.resourceId;
            }
            
            fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': this.options.apiToken
                },
                body: JSON.stringify(formData)
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    this.showAlert('Recurso guardado exitosamente', 'success');
                    setTimeout(() => {
                        window.location.href = this.getPackageUrl();
                    }, 1500);
                } else {
                    throw new Error(data.error.message || 'Error al guardar recurso');
                }
            })
            .catch(error => {
                this.handleError('Error al guardar: ' + error.message);
            })
            .finally(() => {
                this.hideSaveLoading();
                this.uploadInProgress = false;
            });
        },

        showProgress: function(percent) {
            this.progressContainer.style.display = 'block';
            this.progressBar.style.width = percent + '%';
            this.progressText.textContent = percent + '%';
        },

        hideProgress: function() {
            this.progressContainer.style.display = 'none';
        },

        showSaveLoading: function() {
            this.saveButton.disabled = true;
            this.saveButton.querySelector('.save-text').classList.add('cloudstorage-hidden');
            this.saveButton.querySelector('.save-loading').classList.remove('cloudstorage-hidden');
        },

        hideSaveLoading: function() {
            this.saveButton.disabled = false;
            this.saveButton.querySelector('.save-text').classList.remove('cloudstorage-hidden');
            this.saveButton.querySelector('.save-loading').classList.add('cloudstorage-hidden');
        },

        showAlert: function(message, type) {
            const alertHtml = `
                <div class="cloudstorage-alert cloudstorage-alert-${type}">
                    ${message}
                </div>
            `;
            this.alertContainer.innerHTML = alertHtml;
            
            setTimeout(() => {
                this.alertContainer.innerHTML = '';
            }, 5000);
        },

        handleError: function(message) {
            console.error('CloudStorage Resource Editor Error:', message);
            this.showAlert(message, 'error');
        },

        formatFileSize: function(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        },

        getPackageUrl: function() {
            return '/dataset/' + this.options.packageId;
        }
    };

    // Initialize when DOM is ready
    document.addEventListener('DOMContentLoaded', function() {
        const container = document.getElementById('CloudStorageResourceEditor');
        if (container) {
            const options = {
                packageId: container.dataset.packageId || '',
                resourceId: container.dataset.resourceId || '',
                apiToken: container.dataset.apiToken || '',
                baseUrl: container.dataset.baseUrl || '',
                uploadType: container.dataset.uploadType || 'multipart'
            };
            
            new CloudStorageResourceEditor('CloudStorageResourceEditor', options);
        }
    });

    // Export for global use
    window.CloudStorageResourceEditor = CloudStorageResourceEditor;

})(); 