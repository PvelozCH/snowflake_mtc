# Proyecto de Extracción de Datos de Snowflake

Este proyecto contiene una serie de scripts de Python para extraer datos de comentarios y órdenes de trabajo desde una instancia de Snowflake, procesarlos y almacenarlos en una base de datos local SQLite. También genera archivos JSON a partir de los datos almacenados.

## Descripción General

El flujo de trabajo principal es el siguiente:

1.  **Conexión**: El script principal (`main.py`) se conecta a Snowflake y a una base de datos local SQLite (`BDD_SNOWFLAKE.db`).
2.  **Extracción**: Se ejecutan consultas SQL para obtener datos de órdenes de trabajo (OT) y comentarios desde Snowflake.
3.  **Procesamiento**: Los datos se procesan y se insertan en tablas locales en SQLite. Se utiliza un hash MD5 para identificar registros únicos y evitar duplicados. Las imágenes asociadas a los comentarios se descargan en la carpeta `carpeta_imagenes`.
4.  **Generación de JSON**: Los scripts pueden generar archivos JSON a partir de los datos almacenados en SQLite para su uso en otros sistemas.

## Archivos del Proyecto

-   `main.py`: El punto de entrada de la aplicación. Orquesta todo el proceso de extracción y procesamiento.
-   `snowflake_servicios.py`: Un módulo de servicio que contiene la lógica principal para interactuar con Snowflake, procesar los datos, descargar imágenes y manejar la base de datos SQLite.
-   `carga_servicios.py`: Contiene funciones para convertir los datos de la base de datos SQLite a formato JSON.
-   `automatic.py`: (Propósito a ser documentado).
-   `BDD_SNOWFLAKE.db`: La base de datos SQLite que se crea para almacenar los datos localmente.
-   `Log.txt`: Un archivo de log que registra los eventos y errores durante la ejecución.

## Requisitos

-   Python 3.x
-   Las librerías especificadas en `requirements.txt` (si existe) o las importadas en los scripts (e.g., `snowflake-connector-python`, `snowflake-snowpark-python`, `selenium`).

## Cómo Usar

El script `main.py` se ejecuta desde la línea de comandos y requiere un parámetro para definir el modo de operación.

### Modos de Ejecución

1.  **Modo Histórico (`historico`)**:
    Realiza una carga completa de todos los datos desde Snowflake. Es ideal para la primera ejecución o para una recarga total.

    ```bash
    python main.py historico
    ```

2.  **Modo Temporal/Incremental (`temp`)**:
    Realiza una carga incremental, procesando únicamente los registros que son nuevos desde la última ejecución.

    ```bash
    python main.py temp
    ```

3.  **Generar JSON Histórico (`jsonhistorico`)**:
    Omite la conexión con Snowflake y simplemente genera el archivo JSON histórico a partir de los datos ya existentes en la base de datos SQLite.

    ```bash
    python main.py jsonhistorico
    ```

## Configuración

Las credenciales de conexión a Snowflake se encuentran actualmente codificadas en el archivo `main.py`. Para un entorno de producción, se recomienda encarecidamente mover estas credenciales a un método más seguro, como variables de entorno o un archivo de configuración `.env`.
