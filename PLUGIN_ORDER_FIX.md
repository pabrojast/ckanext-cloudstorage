# CloudStorage Plugin Order Fix

## IMPORTANTE: Orden de plugins en production.ini

Para que CloudStorage funcione correctamente con SchemingDCAT, el orden de los plugins en `ckan.plugins` debe ser:

```ini
ckan.plugins = ... schemingdcat_datasets ... cloudstorage ...
```

**CloudStorage DEBE estar DESPUÉS de schemingdcat_datasets**

## Configuración actual (INCORRECTA):
```ini
ckan.plugins = cloudstorage authz_service doi auth asset_storage envvars theme_ejemplo image_view text_view recline_view datastore pdf_view resource_proxy terria_view pages spatial_metadata spatial_query stats colab ckan_harvester dcat dcat_rdf_harvester dcat_json_harvester dcat_json_interface structured_data video_view schemingdcat schemingdcat_datasets harvest schemingdcat_ckan_harvester schemingdcat_groups schemingdcat_organizations fluent package_group_permissions datapusher
```

## Configuración correcta:
```ini
ckan.plugins = authz_service doi auth asset_storage envvars theme_ejemplo image_view text_view recline_view datastore pdf_view resource_proxy terria_view pages spatial_metadata spatial_query stats colab ckan_harvester dcat dcat_rdf_harvester dcat_json_harvester dcat_json_interface structured_data video_view schemingdcat schemingdcat_datasets harvest schemingdcat_ckan_harvester schemingdcat_groups schemingdcat_organizations fluent package_group_permissions datapusher cloudstorage
```

## Por qué es importante el orden:

1. SchemingDCAT define sus propios templates para resource forms
2. CloudStorage necesita sobrescribir estos templates
3. CKAN procesa los plugins en orden, y el último plugin cargado tiene prioridad
4. Al poner CloudStorage al final, sus templates y configuraciones sobrescriben las de SchemingDCAT

## Después de cambiar el orden:

1. Actualiza el archivo production.ini con el orden correcto
2. Reinicia CKAN para que tome los cambios
3. Verifica que ahora se muestre la interfaz de CloudStorage en el formulario de recursos