# Plan de Trabajo: Corrección Definitiva del Azure Blob Upload

## Estado: ✅ IMPLEMENTADO

### Fecha de implementación: 2025-12-22

---

## Problema Identificado

### Síntomas
1. **Error intermitente**: "The specified blob does not exist" (BlobNotFound)
2. **HARAKIRI del worker uWSGI**: Timeout en `datastore_create` 
3. **Error 409 Conflict**: Conflictos de concurrencia en datastore
4. **Múltiples intentos de mover el mismo blob**: En entornos multi-worker

### Causa Raíz
Hay una **condición de carrera (race condition)** en el flujo de Azure Direct Upload:

```
FLUJO PROBLEMÁTICO:
1. Frontend sube archivo a Azure: temp/{uuid}/{filename}
2. Frontend llama resource_create con azure_blob_path
3. CKAN crea múltiples instancias de ResourceCloudStorage (en diferentes hooks/callbacks)
4. CADA instancia intenta mover el blob de temp/ a resources/
5. El primero tiene éxito, los siguientes fallan con "blob not found"
6. El cache _azure_completed_moves no es compartido entre workers
```

---

## Solución Implementada

### Principio: "Move Once, Track Globally"
El blob se mueve una única vez, y el estado se persiste en base de datos para ser compartido entre todos los workers.

---

## Componentes Implementados

### 1. Modelo de Estado de Upload (`model.py`)

Nueva clase `AzureUploadStatus` para rastrear el estado de uploads Azure:

```python
class AzureUploadStatus(Base, DomainObject):
    __tablename__ = 'cloudstorage_azure_upload_status'
    
    temp_path = Column(UnicodeText, unique=True, nullable=False)
    final_path = Column(UnicodeText, nullable=True)
    resource_id = Column(UnicodeText, nullable=True)
    status = Column(UnicodeText, default='pending')  # pending, moving, completed, failed
    created = Column(DateTime, default=datetime.utcnow)
    completed = Column(DateTime, nullable=True)
```

**Métodos clave:**
- `get_by_temp_path()`: Busca estado por path temporal
- `create_or_lock()`: Bloqueo atómico usando constraint UNIQUE
- `mark_completed()`: Marca upload como completado
- `mark_failed()`: Marca upload como fallido
- `cleanup_old_entries()`: Limpia entradas antiguas

### 2. Sistema de Bloqueo Distribuido (`storage.py`)

Nuevas funciones helper:

```python
def _is_upload_processed_in_request(temp_path):
    """Verifica si ya se procesó en esta request (Flask g object)"""

def _acquire_move_lock(temp_path, resource_id, final_path):
    """Adquiere bloqueo distribuido via DB, retorna 'acquired', 'already_locked', o 'completed'"""

def _mark_move_completed_db(temp_path, final_path):
    """Marca como completado en DB y cache de memoria"""

def _mark_move_failed_db(temp_path):
    """Marca como fallido en DB"""
```

### 3. Flujo de Upload Refactorizado (`storage.py`)

El método `upload()` ahora:

1. **Verifica request context**: Evita procesar el mismo blob múltiples veces en la misma request
2. **Verifica cache de memoria**: Fast path para duplicados en el mismo proceso
3. **Adquiere bloqueo distribuido**: Usa DB para prevenir race conditions entre workers
4. **Verifica destino primero**: Si ya existe, considera como éxito
5. **Realiza el move**: Solo si tiene el bloqueo
6. **Limpia recursos**: Borra temp blob, actualiza caches, marca en DB

### 4. Comandos CLI (`cli.py`)

Nuevos comandos:

```bash
# Limpiar blobs temporales huérfanos
ckan cloudstorage cleanup-temp-blobs --older-than 24 --dry-run

# Limpiar entradas antiguas de la tabla de estado
ckan cloudstorage cleanup-upload-status --older-than 24
```

### 5. Retry con Backoff Exponencial (Frontend)

En `schemingdcat-batch-upload.js`:

```javascript
_createResourceWithBlobUrl: function(fileItem, callback, retryCount) {
    // Retry hasta 3 veces con delays de 1s, 2s, 4s
    // Solo en errores 5xx o de red
}
```

---

## Archivos Modificados

| Archivo | Cambios |
|---------|---------|
| `ckanext-cloudstorage/ckanext/cloudstorage/model.py` | Añadida clase `AzureUploadStatus` |
| `ckanext-cloudstorage/ckanext/cloudstorage/storage.py` | Refactorizado `upload()`, añadidas funciones de bloqueo |
| `ckanext-cloudstorage/ckanext/cloudstorage/cli.py` | Añadidos comandos `cleanup-temp-blobs`, `cleanup-upload-status` |
| `ckanext-cloudstorage/ckanext/cloudstorage/utils.py` | Añadidas funciones `cleanup_temp_blobs`, `cleanup_upload_status` |
| `ckanext-schemingdcat/.../schemingdcat-batch-upload.js` | Añadido retry con backoff exponencial |

---

## Despliegue

### Pasos para aplicar:

1. **Actualizar código** en el servidor

2. **Reinicializar DB** para crear la nueva tabla:
   ```bash
   ckan cloudstorage initdb
   ```

3. **Reiniciar workers** de CKAN

4. **Opcional: Configurar cron** para limpieza automática:
   ```bash
   # Cada 6 horas, limpiar blobs temporales mayores a 12 horas
   0 */6 * * * cd /srv/app && ckan cloudstorage cleanup-temp-blobs --older-than 12
   
   # Diariamente, limpiar entradas de estado mayores a 24 horas
   0 2 * * * cd /srv/app && ckan cloudstorage cleanup-upload-status --older-than 24
   ```

---

## Testing

### Casos de prueba:
1. ✅ Upload único → debe funcionar sin warnings
2. ✅ Upload múltiple (batch) → todos deben completar
3. ✅ Upload con refresh de página → no debe duplicar
4. ✅ Upload en multi-worker → solo un worker debe mover el blob
5. ✅ Cleanup de temporales → no debe eliminar blobs activos

### Verificación post-despliegue:
```bash
# Verificar que la tabla existe
ckan cloudstorage cleanup-upload-status --older-than 9999

# Verificar blobs temporales
ckan cloudstorage cleanup-temp-blobs --dry-run --older-than 0
```

---

## Beneficios

1. **Elimina race conditions**: Bloqueo distribuido via DB
2. **Reduce logs de error**: No más "blob not found" warnings
3. **Mejora la fiabilidad**: Retry automático en frontend
4. **Facilita debugging**: Tabla de estado para auditoría
5. **Limpieza automática**: Comandos CLI para mantenimiento
