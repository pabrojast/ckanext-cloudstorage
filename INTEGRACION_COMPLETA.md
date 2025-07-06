# Integración Completa CloudStorage + SchemingDCAT

## Problema Identificado

El problema principal era que los scripts JavaScript de CloudStorage buscaban IDs de campos que no existían en el contexto de SchemingDCAT:

- **CloudStorage esperaba**: `#field-image-upload` y `#field-image-url`
- **SchemingDCAT genera**: `#field-resource-upload` y `#field-resource-url`

## Solución Implementada

### 1. **Orden de Plugins (YA APLICADO)**
```ini
ckan.plugins = ... schemingdcat schemingdcat_datasets ... cloudstorage
```
CloudStorage está al final de la lista ✓

### 2. **Actualización de Scripts JavaScript**

#### cloudstorage-multipart-upload.js
- Ahora acepta parámetros `fieldUpload` y `fieldUrl` del módulo
- Por defecto usa los IDs antiguos para compatibilidad
- Puede trabajar con cualquier ID de campo pasado como opción

#### cloudstorage-azure-direct-upload.js
- Misma actualización que multipart
- Soporta IDs personalizados de campos

### 3. **Templates Actualizados**

#### /schemingdcat/form_snippets/upload.html
- Detecta si está en contexto de recursos (field_name='url')
- Para recursos, incluye el template resource_upload_field.html
- Para imágenes, usa los IDs estándar
- Pasa los IDs correctos como parámetros del módulo

#### /schemingdcat/form_snippets/resource_upload_field.html
- Envuelve el campo de upload original de schemingdcat
- Agrega el módulo JavaScript apropiado
- Incluye los assets necesarios

### 4. **Cambios en el Plugin**
- CloudStoragePlugin implementa IDatasetForm con is_fallback=True
- Esto permite que maneje todos los tipos de datasets

## Verificación

Para verificar que funciona:

1. **Reinicia CKAN** después de estos cambios
2. **Accede a** `/dataset/testing-dataset-4/resource/new`
3. **En la consola del navegador (F12)**, deberías ver:
   - Módulos cargados: `cloudstorage-azure-direct-upload` o `cloudstorage-multipart-upload`
   - NO deberías ver errores de "element not found"
4. **Al subir un archivo**:
   - Con Azure Direct: La subida debe ir directamente a `*.blob.core.windows.net`
   - Con S3/Multipart: Debe usar el sistema de chunks

## Depuración

Si aún no funciona:

1. **Verifica en la consola**:
   ```javascript
   // Estos elementos deben existir:
   $('#field-resource-upload').length  // debe ser 1
   $('#field-resource-url').length     // debe ser 1
   ```

2. **Verifica que los módulos se inicializan**:
   - Busca en Network tab archivos JS de cloudstorage
   - Verifica que no hay errores 404

3. **Verifica la configuración**:
   ```ini
   ckanext.cloudstorage.driver = AZURE_BLOBS
   ckanext.cloudstorage.driver_options = {"key": "...", "secret": "..."}
   ckanext.cloudstorage.container_name = data
   ckanext.cloudstorage.azure_direct_upload = 1
   ```

## Resumen de Archivos Modificados

1. `/ckanext/cloudstorage/plugin/__init__.py` - Implementa IDatasetForm
2. `/ckanext/cloudstorage/fanstatic/scripts/cloudstorage-multipart-upload.js` - Soporta IDs personalizados
3. `/ckanext/cloudstorage/fanstatic/scripts/cloudstorage-azure-direct-upload.js` - Soporta IDs personalizados
4. `/ckanext/cloudstorage/templates/schemingdcat/form_snippets/upload.html` - Maneja recursos vs imágenes
5. `/ckanext/cloudstorage/templates/schemingdcat/form_snippets/resource_upload_field.html` - Envuelve campo de recursos
6. `/ckanext/cloudstorage/templates/cloudstorage/snippets/resource_form.html` - Extiende form de schemingdcat