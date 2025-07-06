# Solución para Integración CloudStorage + SchemingDCAT

## El Problema
Los formularios de carga de archivos muestran la interfaz de SchemingDCAT en lugar de CloudStorage.

## Solución Completa

### 1. **Cambiar el orden de plugins en production.ini**
El orden de plugins es CRÍTICO. CloudStorage DEBE estar DESPUÉS de schemingdcat_datasets.

**Configuración actual (INCORRECTA):**
```ini
ckan.plugins = cloudstorage ... schemingdcat_datasets ...
```

**Configuración correcta:**
```ini
ckan.plugins = authz_service doi auth asset_storage envvars theme_ejemplo image_view text_view recline_view datastore pdf_view resource_proxy terria_view pages spatial_metadata spatial_query stats colab ckan_harvester dcat dcat_rdf_harvester dcat_json_harvester dcat_json_interface structured_data video_view schemingdcat schemingdcat_datasets harvest schemingdcat_ckan_harvester schemingdcat_groups schemingdcat_organizations fluent package_group_permissions datapusher cloudstorage
```

### 2. **Verificar la configuración de CloudStorage**
Asegúrate de que tienes TODAS estas líneas en production.ini:

```ini
ckanext.cloudstorage.driver = AZURE_BLOBS
ckanext.cloudstorage.container_name = data
ckanext.cloudstorage.use_secure_urls = 1
ckanext.cloudstorage.azure_direct_upload = 1
ckanext.cloudstorage.driver_options = {"key": "<storage_account_name>", "secret": "<storage_account_key>"}
```

### 3. **Los cambios ya realizados**
Ya hemos modificado el plugin CloudStorage para:
- Implementar `IDatasetForm` con `is_fallback = True`
- Crear templates que extienden los de schemingdcat
- Sobrescribir el campo de upload con la lógica de CloudStorage

### 4. **Pasos para aplicar la solución**

1. **Actualiza production.ini** con el orden correcto de plugins (cloudstorage AL FINAL)
2. **Reinicia CKAN** para que tome los cambios
3. **Verifica** accediendo a `/dataset/testing-dataset-4/resource/new`

### 5. **Cómo verificar que funciona**

Cuando funcione correctamente, verás:
- Si `enhanced_upload` está habilitado: Interfaz con drag & drop y múltiples archivos
- Si `azure_direct_upload = 1`: Subida directa a Azure (evita timeouts)
- Si no: Interfaz multipart estándar de CloudStorage

En el navegador, abre la consola (F12) y deberías ver:
- Carga de scripts: `cloudstorage-azure-direct-upload.js` o `cloudstorage-enhanced-upload.js`
- En Network: Las subidas deben ir a `*.blob.core.windows.net` (si Azure direct está activo)

### 6. **Si aún no funciona**

1. Verifica logs de CKAN para errores de inicialización de CloudStorage
2. Confirma que helm está pasando correctamente `driver_options`
3. Prueba temporalmente deshabilitando caché de templates de CKAN
4. Verifica que el schema usa `preset: resource_url_upload` (ya confirmado que sí)

## Resumen
El cambio principal es el **orden de plugins**. CloudStorage debe cargarse DESPUÉS de schemingdcat_datasets para poder sobrescribir sus templates.