import sys
import sqlite3
from snowflake.snowpark import Session

from snowflake_servicios import crear_ot, crear_comentarios, log
from carga_servicios import jsonHistorico, cargaEndpoint, enviar_carpeta_imagenes_memoria
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
ENDPOINT = "http://localhost:8000/recibir-json"
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
            a.comment_description, a.location_urls, a.comment_used_for, a.created_date
        FROM sw_temp_maintainer_comments AS a
        LEFT JOIN sw_element_instance AS b ON a.element_instance_id = b.id
        WHERE a.comment_used_for IN ('Notification', 'Report')
    )
    SELECT 
        c.id, t.activity_id, t.sap_work_number, c.role_name, c.work_sequence_name,
        c.element_step, c.element_instance_name, c.suffix, c.comment_title,
        c.comment_description, c.location_urls, c.comment_used_for, c.created_date
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
    Modo de carga completa histórica:
    - Procesa todas las OT
    - Descarga todos los comentarios e imágenes
    - Genera JSON histórico
    - Envía todo al endpoint
    """
    print("--- MODO HISTÓRICO ---")
    
    crear_ot(session, QUERY_OT, conn_sqlite)
    crear_comentarios(session, QUERY_COMENTARIOS, conn_sqlite, "historico")
    jsonHistorico()
    
    # Carga de comentarios e imagenes a endpoint
    ''' 
    cargaEndpoint(JSON_HISTORICO, ENDPOINT)

    # Envía todas las imágenes históricas al endpoint
    enviar_carpeta_imagenes_memoria(
        carpeta_imagenes=CARPETA_IMAGENES,
        tipo="historico",
        endpoint=ENDPOINT
    )

    '''
    
    conn_sqlite.commit()
    print("--- PROCESO HISTÓRICO COMPLETADO ---")


def modo_temp(session, conn_sqlite):
    """
    Modo de carga incremental:
    - Actualiza lista de OT
    - Procesa solo comentarios nuevos
    - Descarga y envía imágenes nuevas inmediatamente
    - Genera y envía JSON temporal con los nuevos
    """
    print("--- MODO TEMPORAL (INCREMENTAL) ---")
    
    crear_ot(session, QUERY_OT, conn_sqlite)
    nom_json = crear_comentarios(session, QUERY_COMENTARIOS, conn_sqlite, "temp")
    
    if nom_json:
        cargaEndpoint(nom_json, ENDPOINT)
    else:
        raise ValueError("No hay comentarios nuevos")
    
    conn_sqlite.commit()
    print("--- PROCESO TEMPORAL COMPLETADO ---")


def modo_json_historico(conn_sqlite):
    """
    Genera JSON histórico desde la base de datos local
    (no consulta Snowflake, solo lee de SQLite)
    """
    print("--- MODO GENERAR JSON HISTÓRICO ---")
    
    jsonHistorico()
    conn_sqlite.commit()
    
    print("--- GENERACIÓN DE JSON HISTÓRICO COMPLETADA ---")


def modo_envio_endpoint():
    """
    Modo de prueba: envía datos históricos existentes al endpoint
    (no consulta bases de datos, solo envía archivos)
    """
    print("--- MODO ENVÍO A ENDPOINT ---")
    
    cargaEndpoint(JSON_HISTORICO, ENDPOINT)
    enviar_carpeta_imagenes_memoria(
        carpeta_imagenes=CARPETA_IMAGENES,
        tipo="historico",
        endpoint=ENDPOINT
    )
    
    print("--- ENVÍO COMPLETADO ---")


def main():
    """Punto de entrada principal del programa"""
    # Validar que se proporcione un parámetro
    if len(sys.argv) < 2:
        print("Error: Debe proporcionar un parámetro de ejecución (historico, temp, jsonhistorico).")
        sys.exit(1)
    
    parametro = sys.argv[1].lower()
    
    # Inicializar archivo de log
    log()
    
    # Variables para las conexiones
    session = None
    conn_sqlite = None
    
    try:
        # Modos que requieren Snowflake y SQLite
        if parametro in ["historico", "temp"]:
            session = conectar_snowflake()
            conn_sqlite = conectar_sqlite()
            
            if parametro == "historico":
                modo_historico(session, conn_sqlite)
            else:
                modo_temp(session, conn_sqlite)
        
        # Modo que solo requiere SQLite
        elif parametro == "jsonhistorico":
            conn_sqlite = conectar_sqlite()
            modo_json_historico(conn_sqlite)
        
        # Modo que no requiere conexiones (solo envío)
        elif parametro == "enviojsonendpoint":
            modo_envio_endpoint()
        
        else:
            print(f"Error: Parámetro '{parametro}' no reconocido.")
            sys.exit(1)
    
    finally:
        # Cerrar conexiones de forma segura
        if conn_sqlite:
            conn_sqlite.close()
        if session:
            session.close()


if __name__ == "__main__":
    main()