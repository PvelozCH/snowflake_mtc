import sys
import sqlite3
import os
from snowflake.snowpark import Session

from snowflake_servicios import (
    crear_ot, crear_comentarios, crear_json_temporal, 
    get_pending_comentarios, get_pending_comentario_ids, update_comment_status
)
from carga_servicios import jsonHistorico, cargaEndpoint, enviar_imagenes_de_comentario
from logger_config import logger, start_run_log

CONEXION_SNOWFLAKE = {
    "WAREHOUSE": "DEFAULT_WAREHOUSE",
    "ACCOUNT": "BHP-SYDNEY",
    "USER": "NICOLAS.JIMENEZ1@BHP.COM",
    "AUTHENTICATOR": "externalbrowser",
    "ROLE": "SNOWFLAKE_READER_PROD",
    "DATABASE": "GLOBAL_PROD",
    "SCHEMA": "QA_DU_STANDARDISED_WORK_MINAM"
}

# Configuración de archivos y rutas
DB_SQLITE = "BDD_SNOWFLAKE.db"
ENDPOINT = "https://volcano-soa.metacontrol.cl/api/import/comentarios/historico"
ENDPOINT_IMG = "https://volcano-soa.metacontrol.cl/api/import/comentarios/foto"
JSON_HISTORICO = "2.comentarios_por_ot_historico.json"
CARPETA_IMAGENES = "carpeta_imagenes"

# Query para obtener lista de órdenes de trabajo
QUERY_OT = """
    SELECT DISTINCT activity_id, sap_work_number AS OT
    FROM sw_temp_maintainer_comments 
    WHERE activity_mwc = 'SN16'
"""

# Query para obtener comentarios con sus detalles
QUERY_COMENTARIOS = f"""
    WITH TodasLasOT AS (
        SELECT DISTINCT activity_id, sap_work_number AS OT
        FROM sw_temp_maintainer_comments
        WHERE activity_mwc = 'SN16'
    ),
    ComentariosPorOT AS (
        SELECT 
            a.id, a.activity_id, a.sap_work_number, b.role_name, b.work_sequence_name,
            b.element_step, a.element_instance_name, a.suffix, a.comment_title,
            a.comment_description, a.location_urls, a.comment_used_for, a.created_date,
            a.activity_name
        FROM sw_temp_maintainer_comments AS a
        LEFT JOIN sw_element_instance AS b ON a.element_instance_id = b.id
        WHERE a.comment_used_for IN ('Notification', 'Report')
    )
    SELECT 
        c.id, t.activity_id, t.OT, c.role_name, c.work_sequence_name,
        c.element_step, c.element_instance_name, c.suffix, c.comment_title,
        c.comment_description, c.location_urls, c.comment_used_for, c.created_date,
        c.activity_name
    FROM ComentariosPorOT AS c
    INNER JOIN TodasLasOT AS t ON c.activity_id = t.activity_id AND c.sap_work_number = t.OT
    ORDER BY t.OT, c.id;
"""


def conectar_snowflake():
    """Establece y retorna la conexión con Snowflake"""
    try:
        session = Session.builder.configs(CONEXION_SNOWFLAKE).create()
        logger.info("Conexión exitosa con Snowflake.")
        return session
    except Exception as e:
        logger.exception("Error crítico al conectar con Snowflake. Abortando ejecución.")
        sys.exit(1)


def conectar_sqlite():
    """Establece y retorna la conexión con SQLite"""
    try:
        conn = sqlite3.connect(DB_SQLITE)
        logger.info(f"Conexión exitosa con SQLite en '{DB_SQLITE}'.")
        return conn
    except Exception as e:
        logger.exception(f"Error crítico al conectar con SQLite en '{DB_SQLITE}'. Abortando ejecución.")
        sys.exit(1)


def modo_historico(session, conn_sqlite):
    """
    Modo de carga completa histórica.
    Guarda todos los comentarios con estado 'pendiente'.
    """
    logger.info("--- INICIANDO MODO HISTÓRICO ---")
    
    crear_ot(session, QUERY_OT, conn_sqlite)
    crear_comentarios(session, QUERY_COMENTARIOS, conn_sqlite, "historico")
    jsonHistorico()
    
    conn_sqlite.commit()
    logger.info("--- PROCESO HISTÓRICO COMPLETADO ---")


def procesar_comentario_individual(comentario, conn_sqlite):
    """
    Procesa y envía un único comentario y sus imágenes. Actualiza el estado si todo es exitoso.
    """
    comentario_id = comentario.get('ID')
    nombre_json_temp = None
    try:
        logger.info(f"Procesando comentario ID {comentario_id}...")
        comentario_individual = [comentario]
        nombre_json_temp = crear_json_temporal(comentario_individual)
        
        if not nombre_json_temp:
            logger.error(f"No se pudo generar el archivo JSON temporal para comentario ID {comentario_id}. El comentario seguirá como 'pendiente'.")
            return

        # 1. Enviar datos del comentario
        cargaEndpoint(nombre_json_temp, ENDPOINT)
        logger.info(f"Datos del comentario ID {comentario_id} enviados exitosamente.")

        # 2. Enviar imágenes asociadas
        imagenes_ok = enviar_imagenes_de_comentario(comentario_id, CARPETA_IMAGENES, "temp", ENDPOINT_IMG)
        
        # 3. Actualizar estado solo si todo fue exitoso
        if imagenes_ok:
            update_comment_status(conn_sqlite, comentario_id, "exitoso")
            logger.info(f"Comentario ID {comentario_id} y sus imágenes procesados con éxito. Estado actualizado a 'exitoso'.")
        else:
            logger.warning(f"No todas las imágenes para el comentario ID {comentario_id} se enviaron correctamente. El estado permanecerá como 'pendiente'.")

    except Exception:
        logger.exception(f"Error durante el envío del comentario ID {comentario_id} o sus imágenes. El comentario seguirá como 'pendiente'.")
    
    finally:
        if nombre_json_temp and os.path.exists(nombre_json_temp):
            try:
                os.remove(nombre_json_temp)
                logger.info(f"Archivo JSON temporal '{nombre_json_temp}' eliminado.")
            except OSError as e:
                logger.error(f"No se pudo eliminar el archivo JSON temporal '{nombre_json_temp}': {e}")


def modo_temp(session, conn_sqlite):
    """
    Modo de carga incremental con estado. El procesamiento es atómico por comentario.
    """
    logger.info("--- INICIANDO MODO TEMPORAL (CON ESTADO) ---")
    crear_ot(session, QUERY_OT, conn_sqlite)
    crear_comentarios(session, QUERY_COMENTARIOS, conn_sqlite, "temp")
    
    comentarios_a_enviar = get_pending_comentarios(conn_sqlite)
    
    if not comentarios_a_enviar:
        logger.info("--- PROCESO TEMPORAL COMPLETADO: No hay comentarios pendientes para enviar. ---")
        return

    logger.info(f"Se encontraron {len(comentarios_a_enviar)} comentarios pendientes para procesar.")

    for comentario in comentarios_a_enviar:
        procesar_comentario_individual(comentario, conn_sqlite)
    
    logger.info("--- PROCESO TEMPORAL COMPLETADO ---")


def modo_json_historico(conn_sqlite):
    """
    Genera JSON histórico desde la base de datos local
    (no consulta Snowflake, solo lee de SQLite)
    """
    logger.info("--- INICIANDO MODO GENERAR JSON HISTÓRICO ---")
    jsonHistorico()
    conn_sqlite.commit()
    logger.info("--- GENERACIÓN DE JSON HISTÓRICO COMPLETADA ---")


def modo_envio_endpoint(conn_sqlite):
    """
    Envía datos pendientes existentes al endpoint uno por uno y actualiza el estado.
    """
    logger.info("--- INICIANDO MODO ENVÍO A ENDPOINT (UNO POR UNO) ---")
    
    comentarios_a_enviar = get_pending_comentarios(conn_sqlite)
    
    if not comentarios_a_enviar:
        logger.info("No hay comentarios pendientes en la base de datos para enviar.")
        return

    logger.info(f"Iniciando envío de {len(comentarios_a_enviar)} comentarios pendientes al endpoint...")
    
    for comentario in comentarios_a_enviar:
        procesar_comentario_individual(comentario, conn_sqlite)

    logger.info("--- PROCESO DE ENVÍO A ENDPOINT COMPLETADO ---")


def modo_solo_fotos(conn_sqlite):
    """
    Busca comentarios pendientes y envía solo sus imágenes asociadas, actualizando estado.
    El procesamiento es atómico por comentario.
    """
    logger.info("--- INICIANDO MODO ENVIAR SOLO FOTOS DE PENDIENTES ---")
    
    comentarios_pendientes = get_pending_comentario_ids(conn_sqlite)
    
    if not comentarios_pendientes:
        logger.info("No hay comentarios pendientes. No se enviaron fotos.")
        return

    logger.info(f"Se encontraron {len(comentarios_pendientes)} comentarios pendientes. Procesando sus fotos...")

    for comentario in comentarios_pendientes:
        comentario_id = comentario.get('ID')
        try:
            logger.info(f"Procesando fotos para comentario ID {comentario_id}...")
            
            imagenes_ok = enviar_imagenes_de_comentario(comentario_id, CARPETA_IMAGENES, "temp", ENDPOINT_IMG)
            
            if imagenes_ok:
                # Este modo asume que el dato del comentario ya fue enviado previamente.
                # Si las imágenes son exitosas, se considera el comentario completo.
                update_comment_status(conn_sqlite, comentario_id, "exitoso")
                logger.info(f"Fotos para comentario ID {comentario_id} enviadas con éxito. Estado actualizado a 'exitoso'.")
            else:
                logger.warning(f"Fallo en envío de imágenes para comentario ID {comentario_id}. El estado no se actualizará.")

        except Exception:
            logger.exception(f"Error durante el envío de fotos para el comentario ID {comentario_id}. El comentario seguirá como 'pendiente'.")

    logger.info("--- PROCESO DE ENVÍO DE FOTOS COMPLETADO ---")


def main():
    if len(sys.argv) < 2:
        print("Error: Debe proporcionar un parámetro de ejecución (historico, temp, jsonhistorico, enviojsonendpoint, solofotos).", file=sys.stderr)
        sys.exit(1)
    
    parametro = sys.argv[1].lower()
    start_run_log(parametro)
    
    session = None
    conn_sqlite = None
    
    try:
        if parametro in ["historico", "temp"]:
            session = conectar_snowflake()
            conn_sqlite = conectar_sqlite()
            
            if parametro == "historico":
                modo_historico(session, conn_sqlite)
            else:
                modo_temp(session, conn_sqlite)
        
        elif parametro in ["jsonhistorico", "enviojsonendpoint", "solofotos"]:
            conn_sqlite = conectar_sqlite()
            if parametro == "jsonhistorico":
                modo_json_historico(conn_sqlite)
            elif parametro == "enviojsonendpoint":
                modo_envio_endpoint(conn_sqlite)
            else:
                modo_solo_fotos(conn_sqlite)
        
        else:
            logger.error(f"Parámetro '{parametro}' no reconocido. Use uno de: historico, temp, jsonhistorico, enviojsonendpoint, solofotos.")
            sys.exit(1)
    
    finally:
        if conn_sqlite:
            conn_sqlite.close()
            logger.info("Conexión a SQLite cerrada.")
        if session:
            session.close()
            logger.info("Conexión a Snowflake cerrada.")


if __name__ == "__main__":
    main()