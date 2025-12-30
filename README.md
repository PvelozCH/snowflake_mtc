# Ejecución del Proceso de Sincronización
## Uso General

El programa se ejecuta desde la línea de comandos de la siguiente manera:

```bash
.\python.exe main.py [parametro]
```

## Parámetros Disponibles

A continuación se detallan los parámetros válidos y las acciones que ejecuta cada uno.

---

### `historico`

Realiza una carga completa de todos los datos desde Snowflake a la base de datos local.

**Comando:**
```bash
.\python.exe main.py historico
```

**Acciones Ejecutadas:**
1.  **`crear_ot`**: Se conecta a Snowflake y sincroniza la lista de órdenes de trabajo en la base de datos local SQLite.
2.  **`crear_comentarios(..., "historico")`**: Descarga todos los comentarios y sus imágenes asociadas desde Snowflake. Los comentarios se guardan en SQLite con el estado por defecto `"pendiente"`.
3.  **`jsonHistorico`**: Genera el archivo `2.comentarios_por_ot_historico.json` a partir de todos los registros en la tabla `comentarios` de SQLite.
4.  **(Simulación de envío)**: Al final del proceso, se prepara para enviar los datos al endpoint. Si el envío fuese exitoso, se actualizaría el estado de **todos los comentarios** en la base de datos a `"exitoso"`.

---

### `temp`

Realiza una carga incremental de datos. Sincroniza comentarios nuevos que no existen en la base de datos local y, además, procesa todos los comentarios que quedaron en estado `"pendiente"` de ejecuciones anteriores.

**Comando:**
```bash
.\python.exe main.py temp
```

**Acciones Ejecutadas:**
1.  **`crear_comentarios(..., "temp")`**: Se conecta a Snowflake y busca comentarios que no existan en la base de datos local SQLite. Los nuevos que encuentra los guarda con estado `"pendiente"`.
2.  **`get_pending_comentarios`**: Consulta la base de datos SQLite y obtiene una lista de **todos** los comentarios cuyo estado es `"pendiente"` (esto incluye los recién agregados y los que fallaron en envíos anteriores).
3.  **`crear_json_temporal`**: Crea un archivo JSON (`comentarios_temp_...json`) que contiene únicamente los comentarios pendientes obtenidos en el paso anterior.
4.  **(Simulación de envío)**: Se prepara para enviar el JSON temporal y sus imágenes asociadas al endpoint. Si el envío fuese exitoso, se actualizaría el estado de **solo los comentarios enviados** a `"exitoso"`.

---

### `jsonhistorico`

Regenera el archivo `2.comentarios_por_ot_historico.json` utilizando únicamente los datos que ya están en la base de datos local SQLite. **No se conecta a Snowflake.**

**Comando:**
```bash
.\python.exe main.py jsonhistorico
```
**Acciones Ejecutadas:**
1.  **`jsonHistorico`**: Lee la tabla `comentarios` de SQLite y sobrescribe el archivo JSON histórico.

---

### `enviojsonendpoint`

Es un modo de prueba diseñado para enviar el archivo `2.comentarios_por_ot_historico.json` y las imágenes existentes al endpoint. **No se conecta a Snowflake.**

**Comando:**
```bash
.\python.exe main.py enviojsonendpoint
```
**Acciones Ejecutadas:**
1.  **(Simulación de envío)**: Se prepara para enviar los datos históricos al endpoint.
2.  **(Simulación de actualización)**: Si el envío fuese exitoso, se conectaríá a la base de datos SQLite para actualizar el estado de **todos los comentarios** a `"exitoso"`.