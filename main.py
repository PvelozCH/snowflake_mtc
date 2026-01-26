import sys
import sqlite3
import os
from snowflake.snowpark import Session

from snowflake_servicios import crear_ot, crear_comentarios, log, crear_json_temporal, get_pending_comentarios, update_status_exitoso
from carga_servicios import jsonHistorico, cargaEndpoint, enviar_carpeta_imagenes_memoria, enviar_imagen_json_memoria
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
#ENDPOINT = "http://201.236.128.151:8989/api/import/comentarios/historico"
ENDPOINT_IMG = "https://volcano-soa.metacontrol.cl/api/import/comentarios/foto"
JSON_HISTORICO = "2.comentarios_por_ot_historico.json"
CARPETA_IMAGENES = "carpeta_imagenes"

# Query para obtener lista de órdenes de trabajo
QUERY_OT = """
    SELECT DISTINCT activity_id, sap_work_number 
    FROM sw_temp_maintainer_comments 
    WHERE activity_mwc = 'SN16'
"""

# Query para obtener comentarios con sus detalles
QUERY_COMENTARIOS = f"""
    WITH TodasLasOT AS (
        SELECT DISTINCT activity_id, sap_work_number
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
        c.id, t.activity_id, t.sap_work_number, c.role_name, c.work_sequence_name,
        c.element_step, c.element_instance_name, c.suffix, c.comment_title,
        c.comment_description, c.location_urls, c.comment_used_for, c.created_date,
        c.activity_name
    FROM ComentariosPorOT AS c
    INNER JOIN TodasLasOT AS t ON c.activity_id = t.activity_id AND c.sap_work_number = t.sap_work_number
    ORDER BY t.sap_work_number, c.id;
"""


def conectar_snowflake():
    """Establece y retorna la conexión con Snowflake"""
    try:
        session = Session.builder.configs(CONEXION_SNOWFLAKE).create()
        print("Se conectó a SnowFlake!")
        return session
    except Exception as e:
        print(f"Error al conectar a Snowflake: {e}")
        sys.exit(1)


def conectar_sqlite():
    """Establece y retorna la conexión con SQLite"""
    try:
        conn = sqlite3.connect(DB_SQLITE)
        print("Se conectó a sqlite!")
        return conn
    except Exception as e:
        print(f"Error al conectar a SQLite: {e}")
        sys.exit(1)


def modo_historico(session, conn_sqlite):
    """
    Modo de carga completa histórica.
    Guarda todos los comentarios con estado 'pendiente' y luego simula un envío.
    """
    print("--- MODO HISTÓRICO ---")
    
    # Sincroniza todos los comentarios y los guarda como 'pendiente'
    crear_ot(session, QUERY_OT, conn_sqlite)
    crear_comentarios(session, QUERY_COMENTARIOS, conn_sqlite, "historico")
    jsonHistorico()
    
    # Envío de comentarios por endpoint
    '''
    try:
        conn_sqlite.row_factory = sqlite3.Row
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT * FROM comentarios")
        todos_los_comentarios = [dict(row) for row in cursor.fetchall()]
        conn_sqlite.row_factory = None
        
        print(f"--- Iniciando envío de {len(todos_los_comentarios)} comentarios históricos ---")
        cargaEndpoint(JSON_HISTORICO, ENDPOINT)
        enviar_carpeta_imagenes_memoria(CARPETA_IMAGENES, "historico", ENDPOINT)
        
        print("--- Envío histórico exitoso, actualizando estado en la base de datos. ---")
        update_status_exitoso(conn_sqlite, todos_los_comentarios)

    except Exception as e:
        print(f"--- ERROR DURANTE EL ENVÍO HISTÓRICO: {e}. El estado no se actualizará. ---")
    '''

    conn_sqlite.commit()
    print("--- PROCESO HISTÓRICO COMPLETADO ---")


def modo_temp(session, conn_sqlite):
    """
    Modo de carga incremental con estado. El procesamiento es atómico por comentario.
    """
    print("--- MODO TEMPORAL (CON ESTADO) ---")
    crear_ot(session, QUERY_OT, conn_sqlite)
    crear_comentarios(session, QUERY_COMENTARIOS, conn_sqlite, "temp")
    
    comentarios_a_enviar = get_pending_comentarios(conn_sqlite)
    
    if not comentarios_a_enviar:
        conn_sqlite.commit()
        print("--- PROCESO TEMPORAL COMPLETADO: No hay comentarios pendientes para enviar. ---")
        return

    print(f"--- Hay {len(comentarios_a_enviar)} comentarios pendientes para procesar. ---")
    comentarios_exitosos = []

    for comentario in comentarios_a_enviar:
        comentario_individual = [comentario]
        nombre_json_temp = None # Initialize outside try
        try:
            nombre_json_temp = crear_json_temporal(comentario_individual)
            
            if not nombre_json_temp:
                print(f"--- ERROR: No se pudo generar el archivo JSON temporal para comentario ID {comentario.get('ID')}. El comentario seguirá como 'pendiente'. ---")
                continue # Skip to next comment
            
            print(f"--- Procesando comentario ID {comentario.get('ID')}... ---")
            cargaEndpoint(nombre_json_temp, ENDPOINT)
            enviar_imagenes_nuevas(comentario_individual, CARPETA_IMAGENES, ENDPOINT_IMG)
            
            comentarios_exitosos.append(comentario)
            print(f"--- Comentario ID {comentario.get('ID')} enviado con éxito. ---")

        except Exception as e:
            print(f"--- ERROR durante el envío del comentario ID {comentario.get('ID')}: {e}. El comentario seguirá como 'pendiente'. ---")
        
        finally:
            if nombre_json_temp and os.path.exists(nombre_json_temp):
                os.remove(nombre_json_temp)

    if comentarios_exitosos:
        print(f"--- Envío finalizado. Actualizando estado para {len(comentarios_exitosos)} comentarios exitosos. ---")
        update_status_exitoso(conn_sqlite, comentarios_exitosos)
    
    conn_sqlite.commit()
    print("--- PROCESO TEMPORAL COMPLETADO ---")


def enviar_imagenes_nuevas(nuevos_comentarios, carpeta_imagenes, endpoint):
    """
    Recorre la lista de comentarios nuevos y envía solo las imágenes asociadas.
    """
    # Adjusted print statement for clarity, as it now often receives a single comment
    print(f"Buscando imágenes para {len(nuevos_comentarios)} comentario(s)...")
    for comentario in nuevos_comentarios:
        comment_id = comentario.get('ID')
        if not comment_id:
            continue

        # Asumir un número máximo de imágenes por comentario para buscar (e.g., 20)
        for i in range(1, 21):
            nombre_img = f"{comment_id}_{i}.jpg"
            ruta_img = os.path.join(carpeta_imagenes, nombre_img)
            
            if os.path.exists(ruta_img):
                print(f"Enviando imagen encontrada: {ruta_img}")
                enviar_imagen_json_memoria(
                    ruta_imagen=ruta_img,
                    tipo="temp",
                    endpoint=endpoint
                )
            else:
                # Si no se encuentra la imagen N, se asume que no hay N+1 y se detiene la búsqueda
                break

def modo_json_historico(conn_sqlite):
    """
    Genera JSON histórico desde la base de datos local
    (no consulta Snowflake, solo lee de SQLite)
    """
    print("--- MODO GENERAR JSON HISTÓRICO ---")
    
    jsonHistorico()
    conn_sqlite.commit()
    
    print("--- GENERACIÓN DE JSON HISTÓRICO COMPLETADA ---")


# Para enviar al endpoint toda la data historica (parametro de prueba)
def modo_envio_endpoint(conn_sqlite):
    """
    Modo de prueba: envía datos históricos existentes al endpoint
    y actualiza el estado de todos los comentarios a 'exitoso' si no hay error.
    EL PROCESAMIENTO ES ATÓMICO POR LOTE.
    """
    print("--- MODO ENVÍO A ENDPOINT (LOTE COMPLETO) ---")
    
    try:
        conn_sqlite.row_factory = sqlite3.Row
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT * FROM comentarios")
        todos_los_comentarios = [dict(row) for row in cursor.fetchall()]
        conn_sqlite.row_factory = None
        
        if not todos_los_comentarios:
            print("No hay comentarios en la base de datos para enviar.")
            conn_sqlite.commit()
            return

        print(f"--- Iniciando envío de {len(todos_los_comentarios)} comentarios al endpoint... ---")
        cargaEndpoint(JSON_HISTORICO, ENDPOINT)
        enviar_carpeta_imagenes_memoria(CARPETA_IMAGENES, "historico", ENDPOINT_IMG)
        
        print("--- Envío de lote completo exitoso, actualizando estado en la base de datos. ---")
        update_status_exitoso(conn_sqlite, todos_los_comentarios)
        
        conn_sqlite.commit()

    except Exception as e:
        print(f"--- ERROR DURANTE EL ENVÍO DEL LOTE: {e}. El estado no se actualizará para ningún comentario. ---")
        conn_sqlite.rollback() # Rollback changes if error
    finally:
        print("--- ENVÍO COMPLETADO ---")


def modo_solo_fotos(conn_sqlite):
    """
    Busca comentarios pendientes y envía solo sus imágenes asociadas.
    El procesamiento es atómico por comentario.
    """
    print("--- MODO ENVIAR SOLO FOTOS DE PENDIENTES ---")
    
    comentarios_pendientes = get_pending_comentarios(conn_sqlite)
    
    if not comentarios_pendientes:
        print("--- No hay comentarios pendientes. No se enviaron fotos. ---")
        conn_sqlite.commit()
        return

    print(f"--- Se encontraron {len(comentarios_pendientes)} comentarios pendientes. Procesando sus fotos... ---")
    comentarios_exitosos = []

    for comentario in comentarios_pendientes:
        try:
            print(f"--- Procesando fotos para comentario ID {comentario.get('ID')}... ---")
            enviar_imagenes_nuevas([comentario], CARPETA_IMAGENES, ENDPOINT_IMG)
            
            comentarios_exitosos.append(comentario)
            print(f"--- Fotos para comentario ID {comentario.get('ID')} enviadas con éxito. ---")

        except Exception as e:
            print(f"--- ERROR durante el envío de fotos para el comentario ID {comentario.get('ID')}: {e}. El comentario seguirá como 'pendiente'. ---")

    if comentarios_exitosos:
        print(f"--- Envío finalizado. Actualizando estado para {len(comentarios_exitosos)} comentarios exitosos. ---")
        update_status_exitoso(conn_sqlite, comentarios_exitosos)
    
    conn_sqlite.commit()
    print("--- PROCESO DE ENVÍO DE FOTOS COMPLETADO ---")


def main():
    if len(sys.argv) < 2:
        print("Error: Debe proporcionar un parámetro de ejecución (historico, temp, jsonhistorico, enviojsonendpoint, solofotos).")
        sys.exit(1)
    
    parametro = sys.argv[1].lower()
    log()
    
    session = None
    conn_sqlite = None
    
    try:
        if parametro in ["historico", "temp"]:
            session = conectar_snowflake()
            conn_sqlite = conectar_sqlite()
            
            if parametro == "historico":
                modo_historico(session, conn_sqlite)
            else: # parametro == "temp"
                modo_temp(session, conn_sqlite)
        
        elif parametro in ["jsonhistorico", "enviojsonendpoint", "solofotos"]:
            conn_sqlite = conectar_sqlite()
            if parametro == "jsonhistorico":
                modo_json_historico(conn_sqlite)
            elif parametro == "enviojsonendpoint":
                modo_envio_endpoint(conn_sqlite)
            else: # parametro == "solofotos"
                modo_solo_fotos(conn_sqlite)
        
        else:
            print(f"Error: Parámetro '{parametro}' no reconocido.")
            sys.exit(1)
    
    finally:
        if conn_sqlite:
            conn_sqlite.close()
            print("Conexión a SQLite cerrada.")
        if session:
            session.close()
            print("Conexión a Snowflake cerrada.")


if __name__ == "__main__":
    main()
