// ============================================================================
// UPLOAD UI
// ============================================================================
// UI initialization, file handling, and user interactions
// 
// This is part of the modular upload system.
// Included by upload_script.html (main) via standard JS includes
// ============================================================================


  function initUpload() {
    var wrapper = document.querySelector('.schemingdcat-upload-wrapper');
    if (!wrapper) return;

    var fileInput = wrapper.querySelector('.upload-file-input');
    var dropzone = wrapper.querySelector('.upload-dropzone');
    var dropzoneContent = wrapper.querySelector('.dropzone-content');
    var filePreview = wrapper.querySelector('.file-preview');
    var removeButton = wrapper.querySelector('.remove-file');
    var previewSyncTimer = null; // to cancel intervals

    if (!fileInput || !dropzone) return;

    // Check if CloudStorage is handling uploads
    var isCloudStorageEnabled = (typeof window.use_azure_direct_upload !== 'undefined' && window.use_azure_direct_upload) ||
                                (typeof window.cloudStorageActive !== 'undefined' && window.cloudStorageActive);
    
    if (isCloudStorageEnabled) {
      console.log('[schemingdcat-upload] CloudStorage is enabled, running in compatibility mode');
      // Hide our UI elements that conflict with CloudStorage
      if (filePreview) {
        filePreview.style.display = 'none';
      }
    }

    // Setup format field listener for spatial processing
    setupFormatFieldListener();
    
    // Auto-fill date field if exists and is empty
    // Look for resource date fields specifically
    var dateFields = document.querySelectorAll('input[type="date"]');
    dateFields.forEach(function(field) {
      if ((field.name.indexOf('created') !== -1 || field.name.indexOf('__0__created') !== -1) && !field.value) {
        var today = new Date();
        var yyyy = today.getFullYear();
        var mm = today.getMonth() + 1;
        var dd = today.getDate();
        
        if (dd < 10) dd = '0' + dd;
        if (mm < 10) mm = '0' + mm;
        
        field.value = yyyy + '-' + mm + '-' + dd;
      }
    });

    // Function to auto-fill name field with filename (similar to CloudStorage functionality)
    function autoFillNameField(fileName, forceUpdate) {
      // Extract name without extension
      var nameWithoutExtension = fileName.replace(/\.[^/.]+$/, '');
      
      // Clean name: replace underscores and hyphens with spaces, normalize whitespace
      var cleanName = nameWithoutExtension.replace(/[_-]/g, ' ').replace(/\s+/g, ' ').trim();
      
      // Find form fields to populate
      var form = wrapper.closest('form');
      if (form) {
        // Priority order for name field detection
        var nameSelectors = [
          'input[name="name"]',
          'input[name="title"]', 
          '#field-name',
          '#field-title',
          'input[id*="name"]',
          'input[id*="title"]',
          'input[name*="name"]',
          'input[name*="title"]'
        ];
        
        var nameField = null;
        for (var i = 0; i < nameSelectors.length; i++) {
          nameField = form.querySelector(nameSelectors[i]);
          if (nameField) break;
        }
        
        // Update field if it's empty OR if we're forcing an update (new file upload)
        if (nameField && (!nameField.value.trim() || forceUpdate)) {
          nameField.value = cleanName;
          nameField.setAttribute('data-auto-filled', 'true'); // Mark as auto-filled
          nameField.dispatchEvent(new Event('change', { bubbles: true }));
          nameField.dispatchEvent(new Event('input', { bubbles: true }));
          
          // Add visual feedback
          nameField.style.backgroundColor = '#d4edda';
          setTimeout(function() {
            nameField.style.backgroundColor = '';
          }, 2000);
          
          console.log('[schemingdcat-upload] Auto-populated name field:', cleanName);
        }
      }
    }

    // Function to clear auto-filled fields when removing a file
    function clearAutoFilledFields() {
      var form = wrapper.closest('form');
      if (form) {
        // Clear name field if it was auto-filled
        var nameSelectors = [
          'input[name="name"]',
          'input[name="title"]', 
          '#field-name',
          '#field-title',
          'input[id*="name"]',
          'input[id*="title"]',
          'input[name*="name"]',
          'input[name*="title"]'
        ];
        
        var nameField = null;
        for (var i = 0; i < nameSelectors.length; i++) {
          nameField = form.querySelector(nameSelectors[i]);
          if (nameField) break;
        }
        
        if (nameField && nameField.hasAttribute('data-auto-filled')) {
          nameField.value = '';
          nameField.removeAttribute('data-auto-filled');
          nameField.dispatchEvent(new Event('change', { bubbles: true }));
          nameField.dispatchEvent(new Event('input', { bubbles: true }));
        }
        
        // Clear format field if it was auto-filled
        var formatFields = document.querySelectorAll('input[name*="format"]');
        formatFields.forEach(function(field) {
          if ((field.name.indexOf('resources') !== -1 || field.name === 'format') && 
              field.hasAttribute('data-auto-filled')) {
            field.value = '';
            field.removeAttribute('data-auto-filled');
            field.dispatchEvent(new Event('change', { bubbles: true }));
          }
        });
        
        // Clear mimetype field if it was auto-filled
        var mimetypeFields = document.querySelectorAll('input[name*="mimetype"]');
        mimetypeFields.forEach(function(field) {
          if ((field.name.indexOf('resources') !== -1 || field.name === 'mimetype') && 
              field.hasAttribute('data-auto-filled')) {
            field.value = '';
            field.removeAttribute('data-auto-filled');
            field.dispatchEvent(new Event('change', { bubbles: true }));
          }
        });
        
        // Clear encoding field if it was auto-filled
        var encodingFields = document.querySelectorAll('input[name*="encoding"]');
        encodingFields.forEach(function(field) {
          if ((field.name.indexOf('resources') !== -1 || field.name === 'encoding') && 
              field.hasAttribute('data-auto-filled')) {
            field.value = '';
            field.removeAttribute('data-auto-filled');
            field.dispatchEvent(new Event('change', { bubbles: true }));
          }
        });
        
        // Clear description field if it was auto-filled
        var descriptionSelectors = [
          'textarea[name="description"]',
          'textarea[name*="description"]',
          '#field-description',
          'textarea[id*="description"]'
        ];
        
        var descriptionField = null;
        for (var i = 0; i < descriptionSelectors.length; i++) {
          descriptionField = form.querySelector(descriptionSelectors[i]);
          if (descriptionField) break;
        }
        
        if (descriptionField && descriptionField.hasAttribute('data-auto-filled')) {
          descriptionField.value = '';
          descriptionField.removeAttribute('data-auto-filled');
          descriptionField.dispatchEvent(new Event('change', { bubbles: true }));
          descriptionField.dispatchEvent(new Event('input', { bubbles: true }));
        }
        
        console.log('[schemingdcat-upload] Cleared auto-filled fields');
      }
    }

    // File input change - only handle if CloudStorage is not active
    fileInput.addEventListener('change', function(e) {
      // If CloudStorage is active, let it handle everything
      if (isCloudStorageEnabled || (window.cloudStorageUploads && Object.keys(window.cloudStorageUploads).length > 0)) {
        console.log('[schemingdcat-upload] CloudStorage is handling this upload');
        // Only do field auto-population, not file display
        var files = e.target.files;
        if (files && files.length > 0) {
          var fileName = files[0].name;
          var ext = fileName.split('.').pop().toLowerCase();
          
          // Auto-fill name field
          autoFillNameField(fileName, true);
          
          // Auto-fill format and other fields
          autoFillFormatFields(ext);
        }
        return; // Exit early, let CloudStorage handle the rest
      }
      
      // Prevent duplicate processing
      if (fileInput.hasAttribute('data-processing')) {
        console.log('[schemingdcat-upload] File input already processing, skipping');
        return;
      }
      
      // Mark as processing to prevent duplicates
      fileInput.setAttribute('data-processing', 'true');
      
      var files = e.target.files;
      if (files && files.length > 0) {
        // Clear any previous file/upload before showing the new one
        if (filePreview.style.display !== 'none') {
          clearFile();
          // Small pause for user to see the transition
          setTimeout(function() {
            processNewFile(files[0]);
            // Remove processing flag
            fileInput.removeAttribute('data-processing');
          }, 100);
        } else {
          processNewFile(files[0]);
          // Remove processing flag
          setTimeout(function() {
            fileInput.removeAttribute('data-processing');
          }, 100);
        }
      } else {
        // No file selected (user might have cancelled), don't block buttons
        var uploadId = wrapper.getAttribute('data-upload-id');
        if (uploadId) {
          unregisterUpload(uploadId);
          wrapper.removeAttribute('data-upload-id');
        }
        // Remove processing flag
        fileInput.removeAttribute('data-processing');
      }
    });
    
    function autoFillFormatFields(ext) {
      // Auto-fill format field if exists
      var formatFields = document.querySelectorAll('input[name*="format"]');
      var formatField = null;
      formatFields.forEach(function(field) {
        if (field.name.indexOf('resources') !== -1 || field.name === 'format') {
          formatField = field;
        }
      });
      
      if (formatField) {
        var extUpper = ext.toUpperCase();
        
        // Format mapping
        var formatMap = {
          'CSV': 'CSV',
          'XLS': 'XLS', 
          'XLSX': 'XLS',
          'JSON': 'JSON',
          'GEOJSON': 'GeoJSON',
          'XML': 'XML',
          'RDF': 'RDF',
          'PDF': 'PDF',
          'DOC': 'DOC',
          'DOCX': 'DOC',
          'PPT': 'PPT',
          'PPTX': 'PPT',
          'TXT': 'TXT',
          'ZIP': 'ZIP',
          'TAR': 'TAR',
          'GZ': 'GZ',
          'MP4': 'MP4',
          'AVI': 'AVI',
          'MOV': 'MOV',
          'SHP': 'SHP',
          'KML': 'KML',
          'KMZ': 'KMZ',
          'GML': 'GML',
          'GPKG': 'GPKG',
          'SLD': 'SLD'
        };
        
        formatField.value = formatMap[extUpper] || extUpper;
        formatField.setAttribute('data-auto-filled', 'true');
        var event = new Event('change', { bubbles: true });
        formatField.dispatchEvent(event);
      }
      
      // Auto-fill mimetype field if exists
      var mimetypeFields = document.querySelectorAll('input[name*="mimetype"]');
      var mimetypeField = null;
      mimetypeFields.forEach(function(field) {
        if (field.name.indexOf('resources') !== -1 || field.name === 'mimetype') {
          mimetypeField = field;
        }
      });
      
      if (mimetypeField) {
        getMimeTypeForExtension(ext, function(mimeType) {
          if (mimeType) {
            mimetypeField.value = mimeType;
            mimetypeField.setAttribute('data-auto-filled', 'true');
            var event = new Event('change', { bubbles: true });
            mimetypeField.dispatchEvent(event);
          }
        });
      }
      
      // Auto-fill encoding field if exists
      var encodingFields = document.querySelectorAll('input[name*="encoding"]');
      var encodingField = null;
      encodingFields.forEach(function(field) {
        if (field.name.indexOf('resources') !== -1 || field.name === 'encoding') {
          encodingField = field;
        }
      });
      
      if (encodingField) {
        var currentFormat = formatField ? formatField.value : '';
        getCharsetForFormat(ext, currentFormat, function(charset) {
          if (charset) {
            encodingField.value = charset;
            encodingField.setAttribute('data-auto-filled', 'true');
            var event = new Event('change', { bubbles: true });
            encodingField.dispatchEvent(event);
          }
        });
      }
    }
    
    function processNewFile(file) {
      // Don't process if CloudStorage is active
      if (isCloudStorageEnabled) {
        console.log('[schemingdcat-upload] CloudStorage is active, skipping file processing');
        return;
      }
      
      // Check if already processing this file
      var currentFileName = wrapper.getAttribute('data-current-file');
      if (currentFileName === file.name && filePreview.style.display !== 'none') {
        console.log('[schemingdcat-upload] File already being processed: ' + file.name);
        return;
      }
      wrapper.setAttribute('data-current-file', file.name);
      
      // Disable beforeunload warning as soon as file is selected
      // This prevents "You will lose unsaved changes" message during upload
      if (window.onbeforeunload) {
        window._originalBeforeUnload = window.onbeforeunload;
        window.onbeforeunload = null;
        console.log('[schemingdcat-upload] Disabled beforeunload warning for upload');
      }
      
      // Check if Azure direct upload is enabled
      var isAzureDirectUpload = false;
      if (typeof window.use_azure_direct_upload !== 'undefined' && window.use_azure_direct_upload) {
        isAzureDirectUpload = true;
        console.log('[schemingdcat-upload] Azure direct upload is enabled');
      }
      
      // Don't register temporary uploads for CloudStorage - it handles its own
      if (!cloudStorageActive) {
        // Register a temporary upload to keep buttons disabled
        // This will be replaced by the actual XHR upload registration later
        var tempUploadId = 'temp_' + Date.now();
        registerUpload(tempUploadId, { temporary: true });
        
        // Store temp ID for cleanup
        if (!window._tempUploadIds) window._tempUploadIds = [];
        window._tempUploadIds.push(tempUploadId);
        
        // Clean up temp upload after a short delay (XHR hook will have registered by then)
        setTimeout(function() {
          unregisterUpload(tempUploadId);
          var idx = window._tempUploadIds.indexOf(tempUploadId);
          if (idx > -1) window._tempUploadIds.splice(idx, 1);
        }, 2000);
      }
      
      // For Azure direct upload, mark it for later processing
      if (isAzureDirectUpload) {
        // Mark this as an Azure upload for later processing
        wrapper.setAttribute('data-azure-upload', 'true');
        wrapper.setAttribute('data-azure-file-name', file.name);
        
        // The actual Azure upload will be handled by cloudstorage module
        console.log('[schemingdcat-upload] File marked for Azure direct upload: ' + file.name);
      }
      
      displayFile(file);

      var fileName = file.name;
      var ext = fileName.split('.').pop().toLowerCase();
      
      // Auto-fill name field (force update to overwrite previous values)
      autoFillNameField(fileName, true);
      
      // Auto-fill format and other fields
      autoFillFormatFields(ext);
      
      // Extract spatial extent from geospatial files
      // Note: Para archivos ZIP que pueden contener shapefiles, la extracción se realizará
      // después de crear el dataset, no durante la selección del archivo
      if (ext.toLowerCase() !== 'zip') {
        extractSpatialExtentFromFile(file, ext);
      } else {
        console.log('[schemingdcat-upload] ZIP file detected - spatial extent extraction will be performed after dataset creation');
      }
      
      // Set up format field change listener for spatial processing
      var formatFields = document.querySelectorAll('input[name*="format"]');
      var formatField = null;
      formatFields.forEach(function(field) {
        if (field.name.indexOf('resources') !== -1 || field.name === 'format') {
          formatField = field;
        }
      });
      if (formatField) {
        setupSpatialProcessingOnFormatChange(formatField);
      }
    }

    // Remove file button
    if (removeButton) {
      removeButton.addEventListener('click', function(e) {
        e.preventDefault();
        clearFile();
      });
    }

    // Drag and drop
    dropzone.addEventListener('dragover', function(e) {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add('dragover');
    });

    dropzone.addEventListener('dragleave', function(e) {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove('dragover');
    });

    dropzone.addEventListener('drop', function(e) {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove('dragover');
      
      var files = e.dataTransfer.files;
      if (files && files.length > 0) {
        fileInput.files = files;
        
        // Trigger change event - let the change handler decide what to do
        var event = new Event('change', { bubbles: true });
        fileInput.dispatchEvent(event);
      }
    });

    dropzone.addEventListener('click', function(e) {
      // Avoid opening dialog if click comes from specific buttons or preview area
      var isBrowseBtn = e.target.closest('.browse-button');
      var isRemoveBtn = e.target.closest('.remove-file');
      var isPreviewArea = filePreview.contains(e.target);
      
      if (!isBrowseBtn && !isRemoveBtn && !isPreviewArea) {
        // Open file selector
        fileInput.click();
      }
    });
    
    // URL input change for format detection
    var urlInput = wrapper.querySelector('input[name="{{ field_url }}"]');
    if (urlInput) {
      urlInput.addEventListener('blur', function(e) {
        var url = e.target.value;
        if (url) {
          // Extract filename from URL for auto-fill name field
          var urlPath = url.split('?')[0].split('#')[0]; // Remove query params and fragments
          var fileName = urlPath.split('/').pop(); // Get last part of path
          if (fileName) {
            autoFillNameField(fileName);
          }
          
          // Extract extension from URL
          var urlParts = url.split('?')[0].split('.');
          if (urlParts.length > 1) {
            var ext = urlParts[urlParts.length - 1].toLowerCase();
            var extUpper = ext.toUpperCase();
            
            // Auto-fill format field
            var formatFields = document.querySelectorAll('input[name*="format"]');
            var formatField = null;
            formatFields.forEach(function(field) {
              if (field.name.indexOf('resources') !== -1 || field.name === 'format') {
                formatField = field;
              }
            });
            
            if (formatField && !formatField.value) {
              // Format mapping
              var formatMap = {
                'CSV': 'CSV',
                'XLS': 'XLS', 
                'XLSX': 'XLS',
                'JSON': 'JSON',
                'GEOJSON': 'GeoJSON',
                'XML': 'XML',
                'RDF': 'RDF',
                'PDF': 'PDF',
                'DOC': 'DOC',
                'DOCX': 'DOC',
                'PPT': 'PPT',
                'PPTX': 'PPT',
                'TXT': 'TXT',
                'ZIP': 'ZIP',
                'TAR': 'TAR',
                'GZ': 'GZ',
                'MP4': 'MP4',
                'AVI': 'AVI',
                'MOV': 'MOV',
                'SHP': 'SHP',
                'KML': 'KML',
                'KMZ': 'KMZ',
                'GML': 'GML',
                'GPKG': 'GPKG',
                'SLD': 'SLD'
              };
              
              formatField.value = formatMap[extUpper] || extUpper;
              // Trigger change event
              var event = new Event('change', { bubbles: true });
              formatField.dispatchEvent(event);
            }
            
            // Auto-fill mimetype field
            var mimetypeFields = document.querySelectorAll('input[name*="mimetype"]');
            var mimetypeField = null;
            mimetypeFields.forEach(function(field) {
              if (field.name.indexOf('resources') !== -1 || field.name === 'mimetype') {
                mimetypeField = field;
              }
            });
            
            if (mimetypeField && !mimetypeField.value) {
              getMimeTypeForExtension(ext, function(mimeType) {
                if (mimeType) {
                  mimetypeField.value = mimeType;
                  // Trigger change event
                  var event = new Event('change', { bubbles: true });
                  mimetypeField.dispatchEvent(event);
                }
              });
            }
            
            // Auto-fill encoding field
            var encodingFields = document.querySelectorAll('input[name*="encoding"]');
            var encodingField = null;
            encodingFields.forEach(function(field) {
              if (field.name.indexOf('resources') !== -1 || field.name === 'encoding') {
                encodingField = field;
              }
            });
            
            if (encodingField && !encodingField.value) {
              var currentFormat = formatField ? formatField.value : '';
              getCharsetForFormat(ext, currentFormat, function(charset) {
                if (charset) {
                  encodingField.value = charset;
                  // Trigger change event
                  var event = new Event('change', { bubbles: true });
                  encodingField.dispatchEvent(event);
                }
              });
            }
          }
        }
      });
    }

    function displayFile(file) {
      // Don't display if CloudStorage is active
      if (isCloudStorageEnabled) {
        console.log('[schemingdcat-upload] CloudStorage is active, skipping file display');
        return;
      }
      
      var fileName = filePreview.querySelector('.file-name');
      var fileSize = filePreview.querySelector('.file-size');
      var fileIcon = filePreview.querySelector('.file-icon');
      var progressBar = filePreview.querySelector('.upload-progress');
      var progressText = filePreview.querySelector('.progress-text');
      
      fileName.textContent = file.name;
      fileSize.textContent = formatFileSize(file.size);
      
      // Update icon
      var ext = file.name.split('.').pop().toLowerCase();
      var iconClass = getFileIcon(ext);
      fileIcon.className = 'fa fa-2x file-icon ' + iconClass;
      
      // Show preview, hide dropzone content
      dropzoneContent.style.display = 'none';
      filePreview.style.display = 'block';
      
      // Don't show progress if CloudStorage is active
      if (progressBar && !isCloudStorageEnabled) {
        progressBar.style.display = 'block';
        var progressFill = progressBar.querySelector('.progress-fill');
        if (progressFill) {
          progressFill.style.width = '0%';
        }
        if (progressText) {
          progressText.textContent = 'Uploading...';
        }
      }
      
      // Check clear checkbox if exists
      var clearCheckbox = wrapper.querySelector('input[type="checkbox"][id*="clear"]');
      if (clearCheckbox) {
        clearCheckbox.checked = true;
      }
    }

    function clearFile() {
      fileInput.value = '';
      dropzoneContent.style.display = 'block';
      filePreview.style.display = 'none';
      
      // Clear the current file attribute
      wrapper.removeAttribute('data-current-file');
      wrapper.removeAttribute('data-azure-upload');
      wrapper.removeAttribute('data-azure-file-name');

      // Clear auto-filled form fields
      clearAutoFilledFields();

      // Reset internal progress
      var progressFill = filePreview.querySelector('.progress-fill');
      if (progressFill) {
        progressFill.style.width = '0%';
        progressFill.style.animation = 'progressAnimation 2s ease-in-out infinite, shimmer 1.5s linear infinite';
      }
      var progressText = filePreview.querySelector('.progress-text');
      if (progressText) {
        progressText.textContent = 'Processing...';
      }
      
      // Uncheck clear checkbox if exists
      var clearCheckbox = wrapper.querySelector('input[type="checkbox"][id*="clear"]');
      if (clearCheckbox) {
        clearCheckbox.checked = false;
      }
      
      // Cancel/hide CloudStorage if active and delete file from cloud storage
      var cloudStorageContainers = document.querySelectorAll('.cloudstorage-progress-container');
      cloudStorageContainers.forEach(function(container) {
        // Hide CloudStorage progress container
        container.style.display = 'none';
        
        // Get upload information to delete the file
        var uploadId = container.getAttribute('data-upload-id');
        var fileName = container.getAttribute('data-file-name');
        var resourceId = container.getAttribute('data-resource-id');
        
        if (uploadId && window.cloudStorageUploads && window.cloudStorageUploads[uploadId]) {
          // Mark as cancelled
          window.cloudStorageUploads[uploadId].cancelled = true;
          
          // If it's a multipart upload, try to abort it
          if (window.cloudStorageUploads[uploadId].isMultipart) {
            abortMultipartUpload(uploadId);
          }
        }
        if (uploadId) {
          unregisterUpload(uploadId);
        }
        
        // If we have file information, try to delete it from cloud storage
        if (resourceId && fileName) {
          deleteCloudStorageFile(resourceId, fileName);
        }
      });
      
      // Also clear CloudStorage indicator if it exists
      var cloudStorageIndicator = wrapper.parentElement.querySelector('.cloudstorage-indicator');
      if (cloudStorageIndicator) {
        cloudStorageIndicator.style.display = 'none';
      }
      
      // Dispatch custom event so other modules know it was cancelled
      var cancelEvent = new CustomEvent('schemingdcat:upload-cancelled', {
        detail: { wrapper: wrapper },
        bubbles: true
      });
      wrapper.dispatchEvent(cancelEvent);
      
      // Show temporary cancellation notification
      var notification = document.createElement('div');
      notification.className = 'upload-cancelled-notification';
      notification.innerHTML = '<i class="fa fa-check-circle"></i> ' + 'Upload cancelled';
      dropzone.appendChild(notification);
      
      setTimeout(function() {
        if (notification.parentNode) {
          notification.parentNode.removeChild(notification);
        }
      }, 2000);
    }
    
    // Function to abort multipart upload
    function abortMultipartUpload(uploadId) {
      try {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/action/cloudstorage_abort_multipart', true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        
        var data = JSON.stringify({
          upload_id: uploadId
        });
        
        xhr.onreadystatechange = function() {
          if (xhr.readyState === 4) {
            if (xhr.status === 200) {
              console.debug('Multipart upload aborted successfully:', uploadId);
            } else {
              console.warn('Failed to abort multipart upload:', xhr.status, xhr.responseText);
            }
          }
        };
        
        xhr.send(data);
      } catch (e) {
        console.warn('Error aborting multipart upload:', e);
      }
    }
    
    // Function to delete file from cloud storage
    function deleteCloudStorageFile(resourceId, fileName) {
      try {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/action/cloudstorage_delete_file', true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        
        var data = JSON.stringify({
          resource_id: resourceId,
          filename: fileName
        });
        
        xhr.onreadystatechange = function() {
          if (xhr.readyState === 4) {
            if (xhr.status === 200) {
              console.debug('Cloud storage file deleted successfully:', fileName);
            } else {
              console.warn('Failed to delete cloud storage file:', xhr.status, xhr.responseText);
              // Try alternative method using resource_delete if file was already created as resource
              if (resourceId) {
                deleteResourceAndFile(resourceId);
              }
            }
          }
        };
        
        xhr.send(data);
      } catch (e) {
        console.warn('Error deleting cloud storage file:', e);
      }
    }
    
    // Alternative function to delete complete resource (includes file)
    function deleteResourceAndFile(resourceId) {
      try {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/action/resource_delete', true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        
        var data = JSON.stringify({
          id: resourceId
        });
        
        xhr.onreadystatechange = function() {
          if (xhr.readyState === 4) {
            if (xhr.status === 200) {
              console.debug('Resource and file deleted successfully:', resourceId);
            } else {
              console.warn('Failed to delete resource:', xhr.status, xhr.responseText);
            }
          }
        };
        
        xhr.send(data);
      } catch (e) {
        console.warn('Error deleting resource:', e);
      }
    }

    function formatFileSize(bytes) {
      if (bytes === 0) return '0 Bytes';
      var k = 1024;
      var sizes = ['Bytes', 'KB', 'MB', 'GB'];
      var i = Math.floor(Math.log(bytes) / Math.log(k));
      return (bytes / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i];
    }

    function getFileIcon(ext) {
      var iconMap = {
        'pdf': 'fa-file-pdf-o',
        'doc': 'fa-file-word-o',
        'docx': 'fa-file-word-o',
        'ppt': 'fa-file-powerpoint-o',
        'pptx': 'fa-file-powerpoint-o',
        'xls': 'fa-file-excel-o',
        'xlsx': 'fa-file-excel-o',
        'csv': 'fa-file-text-o',
        'txt': 'fa-file-text-o',
        'json': 'fa-file-code-o',
        'geojson': 'fa-file-code-o',
        'xml': 'fa-file-code-o',
        'rdf': 'fa-file-code-o',
        'kml': 'fa-file-code-o',
        'gml': 'fa-file-code-o',
        'sld': 'fa-file-code-o',
        'zip': 'fa-file-archive-o',
        'tar': 'fa-file-archive-o',
        'gz': 'fa-file-archive-o',
        'kmz': 'fa-file-archive-o',
        'mp4': 'fa-file-video-o',
        'avi': 'fa-file-video-o',
        'mov': 'fa-file-video-o',
        'shp': 'fa-map-o',
        'gpkg': 'fa-map-o',
        'tif': 'fa-file-image-o',
        'tiff': 'fa-file-image-o',
        'jpg': 'fa-file-image-o',
        'jpeg': 'fa-file-image-o',
        'png': 'fa-file-image-o',
        'gif': 'fa-file-image-o'
      };
      return iconMap[ext] || 'fa-file-o';
    }
    
    // Find the file input change handler (usually around line 500-600)
    // Replace the existing change event handler with this:
    
    $('#field-resource-upload, input[type="file"][name="upload"]').off('change').on('change', function(e) {
        // Prevent duplicate processing
        if (this.hasAttribute('data-cloudstorage-processing')) {
            console.log('[schemingdcat-cloudstorage] Already processing, skipping duplicate');
            return;
        }
        
        // Check if this is the same file being processed
        var file = this.files[0];
        if (!file) return;
        
        var currentFile = this.getAttribute('data-current-cloudstorage-file');
        var fileIdentifier = file.name + '_' + file.size + '_' + file.lastModified;
        if (currentFile === fileIdentifier) {
            console.log('[schemingdcat-cloudstorage] Same file already processing: ' + file.name);
            return;
        }
        
        // Mark as processing
        this.setAttribute('data-cloudstorage-processing', 'true');
        this.setAttribute('data-current-cloudstorage-file', fileIdentifier);
        
        var self = this;
        
        // Clear processing flag after a short delay
        setTimeout(function() {
            self.removeAttribute('data-cloudstorage-processing');
        }, 500);
        
        // Call the original handler only once
        handleFileUpload(file);
    });

    // Also modify the handleFileUpload function to prevent duplicate calls
    // Find the handleFileUpload function and add this at the beginning:
    
    function handleFileUpload(file) {
        // Prevent duplicate processing
        if (window._currentCloudStorageUpload === file.name + '_' + file.size) {
            console.log('[schemingdcat-cloudstorage] File already being uploaded: ' + file.name);
            return;
        }
        window._currentCloudStorageUpload = file.name + '_' + file.size;
        
        // Clear the flag after upload completes or fails
        setTimeout(function() {
            window._currentCloudStorageUpload = null;
        }, 60000); // Clear after 1 minute max
        
        // ...rest of the handleFileUpload function...
    }
  }