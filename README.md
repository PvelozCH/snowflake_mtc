## Descripción General

El flujo de trabajo principal es el siguiente:

1.  **Conexión**: El script principal (`main.py`) se conecta a Snowflake y a una base de datos local SQLite (`BDD_SNOWFLAKE.db`).
2.  **Extracción**: Se ejecutan consultas SQL para obtener datos de órdenes de trabajo (OT) y comentarios desde Snowflake.
3.  **Procesamiento**: Los datos se procesan y se insertan en tablas locales en SQLite. Se utiliza un hash MD5 para identificar registros únicos y evitar duplicados. Las imágenes asociadas a los comentarios se descargan en la carpeta `carpeta_imagenes`.
4.  **Generación y Envío de JSON**: Los scripts pueden generar archivos JSON a partir de los datos almacenados en SQLite y enviarlos, junto con las imágenes, a endpoints externos.

---

## Sistema de Logging 

El proyecto utiliza un sistema de logging centralizado y robusto configurado en `logger_config.py`. Este sistema asegura que todos los eventos importantes, advertencias y errores se registren de manera uniforme, facilitando la depuración y el monitoreo.

### `logger_config.py`

Este archivo es clave para la consistencia del logging en todo el proyecto.

*   **Configuración Centralizada:** Define un logger principal que usan todos los módulos.
*   **Formato Detallado:** Cada mensaje de log sigue un formato `Fecha Hora - NIVEL - modulo:línea - Mensaje`. Esto permite identificar con precisión cuándo, dónde y qué ocurrió.
*   **Salida Doble:** Los mensajes se escriben simultáneamente en:
    *   **`Log.txt` (Archivo):** Todos los mensajes de nivel `INFO` o superior se guardan de forma persistente. Cada ejecución se añade al final del archivo.
    *   **Consola:** Los mismos mensajes se muestran en la terminal en tiempo real.
*   **`start_run_log(parametro)`:** Una función de utilidad que marca claramente el inicio de cada ejecución del script principal en el log, indicando el modo de operación utilizado.
---

## Archivos del Proyecto

-   `main.py`: El punto de entrada de la aplicación. Orquesta todo el proceso de extracción y procesamiento utilizando el sistema de logging robusto para registrar su flujo de ejecución y manejo de modos.
-   `snowflake_servicios.py`: Contiene la lógica principal para interactuar con Snowflake, procesar los datos, descargar imágenes y manejar la base de datos SQLite. Todas sus operaciones están integradas con el nuevo sistema de logging para una trazabilidad detallada de extracciones, inserciones y descargas de imágenes.
-   `carga_servicios.py`: Maneja la generación de archivos JSON a partir de los datos SQLite y el envío de estos JSON y las imágenes a los endpoints externos. Utiliza el sistema de logging para registrar el éxito o fracaso de los envíos, incluyendo la gestión robusta de encabezados de autenticación.
-   `logger_config.py`: Módulo que configura y centraliza el sistema de logging del proyecto, como se describe en detalle en la sección anterior.
-   `automatic.py`: Un script independiente que realiza clics automáticos con el mouse cada 30 segundos (propósito específico a documentar por el usuario, posiblemente para mantener una sesión activa).
-   `BDD_SNOWFLAKE.db`: La base de datos SQLite local que se crea para almacenar los datos extraídos.
-   `Log.txt`: Archivo donde se registran todos los eventos de la aplicación, siguiendo el formato estructurado definido en `logger_config.py`. Este archivo es esencial para monitorear las operaciones y diagnosticar problemas.

## Cómo Usar

El script `main.py` se ejecuta desde la línea de comandos y requiere un parámetro para definir el modo de operación.

### Modos de Ejecución

1.  **Modo Histórico (`historico`)**:
    Realiza una carga completa de todos los datos (OTs y comentarios) desde Snowflake. Los guarda en la base de datos local SQLite y descarga todas las imágenes asociadas. Todos los comentarios se insertan con estado `'pendiente'`. Este modo es ideal para la primera ejecución o para una recarga total.

    ```bash
    python main.py historico
    ```

2.  **Modo Temporal/Incremental (`temp`)**:
    Realiza una carga incremental. Procesa únicamente los registros que son nuevos en Snowflake desde la última ejecución y los guarda como `'pendiente'`. Luego, intenta enviar *todos* los comentarios con estado `'pendiente'` (incluyendo los nuevos y los que hayan fallado en ejecuciones anteriores) a los endpoints. Si el envío es exitoso, actualiza su estado a `'exitoso'`.

    ```bash
    python main.py temp
    ```

3.  **Generar JSON Histórico (`jsonhistorico`)**:
    Omite la conexión con Snowflake. Genera el archivo JSON histórico (`2.comentarios_por_ot_historico.json`) directamente a partir de *todos* los datos ya existentes en la base de datos SQLite local.

    ```bash
    python main.py jsonhistorico
    ```

4.  **Envío Completo a Endpoint (`enviojsonendpoint`)**:
    Este modo asume que todos los datos y fotos históricos ya están en la base de datos local y en la `carpeta_imagenes`. Envía en un solo lote el archivo `2.comentarios_por_ot_historico.json` al endpoint de comentarios y luego todas las imágenes de la `carpeta_imagenes` al endpoint de imágenes. Al finalizar, marca todos los comentarios en la base de datos como `'exitoso'`.

    ```bash
    python main.py enviojsonendpoint
    ```

5.  **Enviar solo Fotos Pendientes (`solofotos`)**:
    Busca todos los comentarios en la base de datos local que tienen el estado `'pendiente'`. Para cada uno de estos comentarios, intenta enviar solo sus imágenes asociadas que se encuentren en la `carpeta_imagenes`. Si el envío de las fotos de un comentario es exitoso, su estado se actualiza a `'exitoso'`. Es útil para reintentar la subida de imágenes fallidas o para complementar un proceso donde solo se subieron los comentarios.

    ```bash
    python main.py solofotos
    ```

## Configuración

Las credenciales de conexión a Snowflake (`CONEXION_SNOWFLAKE`) se encuentran actualmente codificadas en el archivo `main.py`. Para un entorno de producción, se recomienda encarecidamente mover estas credenciales a un método más seguro, como variables de entorno o un archivo de configuración `.env`.
El token de autenticación para los endpoints (`ENDPOINT_BEARER_TOKEN`) es buscado en las variables de entorno, y se advierte si se utiliza un valor harcodeado.
