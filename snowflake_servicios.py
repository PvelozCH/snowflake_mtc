# -*- coding: utf-8 -*-
"""
Servicios para interacción con Snowflake y SQLite
Incluye funciones para descargar imágenes, procesar datos y gestionar comentarios
"""
import sqlite3
import hashlib
import ast
import os
import json
import base64
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from logger_config import logger

# ============================================================================
# FUNCIONES DE UTILIDAD GENERAL
# ============================================================================

def generar_md5(*valores):
    """Genera un hash MD5 a partir de múltiples valores concatenados"""
    cadena = "|".join(str(v) for v in valores)
    return hashlib.md5(cadena.encode("utf-8")).hexdigest()


# ============================================================================
# FUNCIONES DE DESCARGA DE IMÁGENES
# ============================================================================

def descarga_img_selenium(url, contImg, ID):
    """
    Descarga una imagen desde una URL usando Selenium en modo headless.
    Verifica si la imagen ya existe antes de descargarla.
    Retorna la ruta del archivo guardado o None si falla.
    """
    nombre = f"{ID}_{contImg}.jpg"
    ruta_destino = os.path.join("carpeta_imagenes", nombre)

    if os.path.exists(ruta_destino):
        logger.info(f"Imagen ya existe, se omite la descarga: {ruta_destino}")
        return ruta_destino

    options = webdriver.EdgeOptions()
    options.add_argument("--headless=new")
    driver = webdriver.Edge(options=options)
    
    try:
        logger.info(f"Iniciando descarga de imagen desde URL: {url}")
        driver.get(url)
        
        wait = WebDriverWait(driver, 20)
        img = wait.until(EC.presence_of_element_located((By.TAG_NAME, "img")))
        src = img.get_attribute("src")
        
        js = """
        const url = arguments[0];
        return fetch(url)
            .then(res => res.blob())
            .then(blob => new Promise((resolve) => {
                const reader = new FileReader();
                reader.onloadend = () => resolve(reader.result.split(',')[1]);
                reader.readAsDataURL(blob);
            }));
        """
        
        base64_data = driver.execute_script(js, src)
        
        if not base64_data:
            logger.error(f"No se pudo obtener la imagen en base64 con JS desde la URL: {url}")
            return None
        
        imagen = base64.b64decode(base64_data)
        
        with open(ruta_destino, "wb") as f:
            f.write(imagen)
        
        logger.info(f"Imagen descargada y guardada exitosamente en: {ruta_destino}")
        return ruta_destino
    
    except Exception:
        logger.exception(f"Error fatal al descargar o procesar la imagen desde la URL: {url}")
        return None
    
    finally:
        driver.quit()


# ============================================================================
# FUNCIONES DE PROCESAMIENTO DE IMÁGENES
# ============================================================================

def procesar_imagenes_historico(location_urls, comment_id):
    """
    Procesa y descarga imágenes asociadas a un comentario.
    """
    if not location_urls:
        return 0
    
    contador_imagenes = 0
    
    try:
        urls = ast.literal_eval(location_urls)
        if not isinstance(urls, list):
            logger.warning(f"El formato de location_urls para el comentario {comment_id} no es una lista: {location_urls}")
            return 0

        cont_img = 1
        for url in urls:
            if not url:
                continue
            
            logger.info(f"Procesando imagen {cont_img}/{len(urls)} para comentario ID {comment_id}...")
            ruta_img = descarga_img_selenium(url, cont_img, comment_id)
            
            if ruta_img:
                cont_img += 1
                contador_imagenes += 1
    
    except (ValueError, SyntaxError):
        logger.error(f"Error de formato en 'location_urls' para comentario ID {comment_id}. Valor: {location_urls}")
    except Exception:
        logger.exception(f"Error inesperado procesando imágenes para comentario ID {comment_id}.")
    
    return contador_imagenes


# ============================================================================
# FUNCIONES DE GESTIÓN DE TABLAS SQLITE
# ============================================================================

def crear_tabla_ot(cursor):
    """Crea la tabla de órdenes de trabajo si no existe"""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ot_lista (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ACTIVITY_ID INTEGER,
        OT TEXT,
        MD5 TEXT UNIQUE
    )
    """)
    logger.info("Tabla 'ot_lista' asegurada en SQLite.")


def crear_tabla_comentarios(cursor):
    """Crea la tabla de comentarios si no existe"""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comentarios (
        id INTEGER PRIMARY KEY,
        ACTIVITY_ID INTEGER,
        OT TEXT,
        ROLE_NAME TEXT,
        WORK_SEQUENCE_NAME TEXT,
        ELEMENT_STEP INTEGER,
        ELEMENT_INSTANCE_NAME TEXT,
        SUFFIX TEXT,
        COMMENT_TITLE TEXT,
        COMMENT_DESCRIPTION TEXT,
        LOCATION_URLS TEXT,
        COMMENT_USED_FOR TEXT,
        CREATED_DATE TEXT,
        MD5 TEXT,
        status TEXT NOT NULL DEFAULT 'pendiente',
        ACTIVITY_NAME TEXT
    )
    """)
    logger.info("Tabla 'comentarios' asegurada en SQLite.")


# ============================================================================
# FUNCIONES DE INSERCIÓN Y CONSULTA EN SQLITE
# ============================================================================

def insertar_ot(conn_sqlite, cursor, activity_id, ot):
    """
    Inserta una orden de trabajo en SQLite. Retorna True si se insertó, False si ya existía.
    """
    firma = generar_md5(activity_id, ot)
    
    try:
        cursor.execute("INSERT INTO ot_lista(ACTIVITY_ID, OT, MD5) VALUES (?,?,?)", (activity_id, ot, firma))
        conn_sqlite.commit()
        logger.info(f"Nueva OT guardada en SQLite: ACTIVITY_ID={activity_id}, OT={ot}")
        return True
    except sqlite3.IntegrityError:
        logger.debug(f"OT ya existente (mismo MD5), omitiendo inserción: ACTIVITY_ID={activity_id}, OT={ot}")
        return False


def insertar_comentario(conn_sqlite, cursor, datos_comentario):
    """Inserta un comentario completo en SQLite. Lanza excepción en caso de error."""
    try:
        cursor.execute("""
            INSERT INTO comentarios(
                id, ACTIVITY_ID, OT, ROLE_NAME, WORK_SEQUENCE_NAME,
                ELEMENT_STEP, ELEMENT_INSTANCE_NAME, SUFFIX, COMMENT_TITLE,
                COMMENT_DESCRIPTION, LOCATION_URLS, COMMENT_USED_FOR, CREATED_DATE,
                MD5, ACTIVITY_NAME
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, datos_comentario)
        conn_sqlite.commit()
        logger.info(f"Nuevo comentario guardado en SQLite: ID={datos_comentario[0]}")
    except sqlite3.IntegrityError:
        logger.warning(f"Intento de insertar comentario duplicado (ID ya existe): ID={datos_comentario[0]}")
        raise # relanzar para que el flujo principal lo maneje


def comentario_existe(cursor, comment_id):
    """Verifica si un comentario ya existe en SQLite por su ID"""
    cursor.execute("SELECT 1 FROM comentarios WHERE id = ?", (comment_id,))
    return cursor.fetchone() is not None


def ot_existe(cursor, firma):
    """Verifica si una OT ya existe en SQLite por su MD5"""
    cursor.execute("SELECT 1 FROM ot_lista WHERE MD5 = ?", (firma,))
    return cursor.fetchone() is not None


# ============================================================================
# FUNCIONES DE GESTIÓN DE ESTADO
# ============================================================================

def get_pending_comentarios(conn_sqlite):
    """Obtiene todos los comentarios con estado 'pendiente' de SQLite."""
    try:
        conn_sqlite.row_factory = sqlite3.Row
        cursor = conn_sqlite.cursor()
        # Se renombra 'id' a 'ID' para que la clave del diccionario coincida con el resto del código.
        cursor.execute("""
            SELECT 
                id AS ID,
                ACTIVITY_ID, 
                OT, 
                ROLE_NAME, 
                WORK_SEQUENCE_NAME, 
                ELEMENT_STEP, 
                ELEMENT_INSTANCE_NAME, 
                SUFFIX, 
                COMMENT_TITLE, 
                COMMENT_DESCRIPTION, 
                LOCATION_URLS, 
                COMMENT_USED_FOR, 
                CREATED_DATE, 
                MD5, 
                status, 
                ACTIVITY_NAME 
            FROM comentarios 
            WHERE status = 'pendiente'
        """)
        rows = cursor.fetchall()
        comentarios = [dict(row) for row in rows]
        logger.info(f"Se encontraron {len(comentarios)} comentarios con estado 'pendiente'.")
        return comentarios
    except sqlite3.OperationalError as e:
        logger.error(f"Error operacional de SQLite: {e}. Es posible que la columna 'OT' no exista en la tabla 'comentarios'. Si estás usando un modo que no sea 'historico' o 'temp', prueba a ejecutar 'historico' primero para actualizar el esquema de la base de datos.")
        return []
    except Exception:
        logger.exception("Error al obtener comentarios pendientes de SQLite.")
        return []
    finally:
        conn_sqlite.row_factory = None

def get_pending_comentario_ids(conn_sqlite):
    """
    Obtiene solo los IDs de los comentarios con estado 'pendiente' de SQLite.
    Es una versión ligera para 'solofotos' que no necesita todos los datos.
    """
    try:
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT id FROM comentarios WHERE status = 'pendiente'")
        rows = cursor.fetchall()
        # Se retorna una lista de diccionarios para mantener la estructura de datos esperada.
        comentarios = [{'ID': row[0]} for row in rows]
        logger.info(f"Se encontraron {len(comentarios)} IDs de comentarios con estado 'pendiente'.")
        return comentarios
    except Exception:
        logger.exception("Error al obtener IDs de comentarios pendientes de SQLite.")
        return []

def update_comment_status(conn_sqlite, comment_id, status):
    """Actualiza el estado de un único comentario en la base de datos."""
    if not comment_id:
        logger.warning("Se intentó actualizar el estado de un comentario sin ID.")
        return

    logger.info(f"Actualizando estado a '{status}' para el comentario ID: {comment_id}...")
    try:
        cursor = conn_sqlite.cursor()
        cursor.execute("UPDATE comentarios SET status = ? WHERE id = ?", (status, comment_id))
        conn_sqlite.commit()
        
        if cursor.rowcount == 0:
            logger.warning(f"No se encontró el comentario con ID {comment_id} para actualizar. No se realizaron cambios.")
        else:
            logger.info(f"{cursor.rowcount} fila(s) de comentario actualizada(s) a '{status}' en la base de datos.")
            
    except Exception:
        logger.exception(f"Error al actualizar el estado para el ID de comentario: {comment_id}")
        conn_sqlite.rollback()


# ============================================================================
# FUNCIONES DE TRANSFORMACIÓN DE DATOS
# ============================================================================

def extraer_datos_comentario(row):
    """Extrae los datos de un row de Snowflake y los convierte en un diccionario limpio"""
    return {
        'comment_id': row["ID"],
        'activity_id': row["ACTIVITY_ID"],
        'OT': row["OT"],
        'role_name': row["ROLE_NAME"],
        'work_sequence_name': row["WORK_SEQUENCE_NAME"],
        'element_step': row["ELEMENT_STEP"],
        'element_instance_name': row["ELEMENT_INSTANCE_NAME"],
        'suffix': row["SUFFIX"],
        'comment_title': row["COMMENT_TITLE"],
        'comment_description': row["COMMENT_DESCRIPTION"],
        'location_urls': row["LOCATION_URLS"],
        'comment_used_for': row["COMMENT_USED_FOR"],
        'created_date': row["CREATED_DATE"],
        'activity_name': row["ACTIVITY_NAME"]
    }


def preparar_datos_insercion(datos):
    """Prepara los datos para inserción en SQLite. Retorna: (tupla_para_insert, firma_md5)"""
    firma = generar_md5(datos['activity_id'], datos['OT'])
    return (
        datos['comment_id'], datos['activity_id'], datos['OT'],
        datos['role_name'], datos['work_sequence_name'], datos['element_step'],
        datos['element_instance_name'], datos['suffix'], datos['comment_title'],
        datos['comment_description'], datos['location_urls'], datos['comment_used_for'],
        datos['created_date'], firma, datos["activity_name"] 
    ), firma

def crear_json_temporal(comentarios_nuevos):
    """Crea un archivo JSON temporal con los comentarios nuevos."""
    if not comentarios_nuevos:
        logger.warning("No se recibieron comentarios nuevos para crear el JSON temporal.")
        return None
    
    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nombre = f"comentarios_temp_{fecha}.json"
    
    try:
        with open(nombre, "w", encoding="utf-8") as f:
            json.dump(comentarios_nuevos, f, ensure_ascii=False, indent=4, default=str)
        logger.info(f"JSON temporal creado '{nombre}' con {len(comentarios_nuevos)} comentarios.")
        return nombre
    except Exception:
        logger.exception(f"Error al crear el archivo JSON temporal '{nombre}'.")
        return None


# ============================================================================
# FUNCIÓN PRINCIPAL: CREAR ÓRDENES DE TRABAJO
# ============================================================================

def crear_ot(session, query_inicio, conn_sqlite):
    """Obtiene las OTs desde Snowflake y las guarda en SQLite."""
    logger.info("Iniciando subproceso: Sincronización de Órdenes de Trabajo (OTs).")
    try:
        cursor = conn_sqlite.cursor()
        crear_tabla_ot(cursor)
        
        logger.info("Ejecutando query de OTs en Snowflake...")
        ot = session.sql(query_inicio)
        rows = ot.collect()
        logger.info(f"Query ejecutada. {len(rows)} OTs recibidas de Snowflake.")
        
        cont = 0
        for row in rows:
            if insertar_ot(conn_sqlite, cursor, row["ACTIVITY_ID"], row["OT"]):
                cont += 1
        
        logger.info(f"Sincronización de OTs finalizada. Total de OTs nuevas guardadas: {cont}")
    
    except Exception:
        logger.exception("Error crítico en el proceso 'crear_ot'.")


# ============================================================================
# FUNCIÓN PRINCIPAL: CREAR COMENTARIOS
# ============================================================================

def crear_comentarios_historico(session, query, conn_sqlite):
    """Procesa comentarios en modo HISTÓRICO: guarda en SQLite y descarga imágenes."""
    cont_nuevos = 0
    cont_imagenes_total = 0
    try:
        os.makedirs("carpeta_imagenes", exist_ok=True)
        cursor = conn_sqlite.cursor()
        crear_tabla_comentarios(cursor)

        logger.info("Ejecutando query de comentarios en Snowflake...")
        comments = session.sql(query)
        rows_comments = comments.collect()
        logger.info(f"Query ejecutada. {len(rows_comments)} comentarios recibidos de Snowflake.")
        
        for i, row in enumerate(rows_comments):
            datos = extraer_datos_comentario(row)
            comment_id = datos['comment_id']
            logger.debug(f"Procesando comentario {i+1}/{len(rows_comments)} - ID: {comment_id}")

            if comentario_existe(cursor, comment_id):
                logger.debug(f"Comentario ID {comment_id} ya existe en SQLite, se omite.")
                continue
            
            datos_insercion, _ = preparar_datos_insercion(datos)
            try:
                insertar_comentario(conn_sqlite, cursor, datos_insercion)
                cont_nuevos += 1
                cont_imagenes_total += procesar_imagenes_historico(datos['location_urls'], comment_id)
            except sqlite3.IntegrityError:
                # Ya logueado en insertar_comentario, no es necesario hacer más.
                pass
        
        logger.info(f"Total de comentarios nuevos guardados en modo histórico: {cont_nuevos}")
        logger.info(f"Total de imágenes descargadas en modo histórico: {cont_imagenes_total}")

    except Exception:
        logger.exception("Error crítico en 'crear_comentarios_historico'.")


def crear_comentarios_temp(session, query, conn_sqlite):
    """Procesa comentarios en modo TEMPORAL: guarda nuevos y descarga sus imágenes."""
    comentarios_nuevos_para_envio = []
    cont_nuevos_guardados = 0
    try:
        os.makedirs("carpeta_imagenes", exist_ok=True)
        cursor = conn_sqlite.cursor()
        crear_tabla_comentarios(cursor)

        logger.info("Ejecutando query de comentarios en Snowflake...")
        comments = session.sql(query)
        rows_comments = comments.collect()
        logger.info(f"Query ejecutada. {len(rows_comments)} comentarios recibidos de Snowflake.")

        for row in rows_comments:
            datos = extraer_datos_comentario(row)
            comment_id = datos['comment_id']
            logger.debug(f"Procesando comentario ID: {comment_id}")

            if comentario_existe(cursor, comment_id):
                logger.debug(f"Comentario ID {comment_id} ya existe en SQLite, se omite.")
                continue
            
            datos_insercion, firma = preparar_datos_insercion(datos)
            try:
                insertar_comentario(conn_sqlite, cursor, datos_insercion)
                cont_nuevos_guardados += 1
                logger.info(f"Nuevo comentario ID {comment_id} guardado, procesando imágenes...")
                procesar_imagenes_historico(datos['location_urls'], comment_id)
                
                if not ot_existe(cursor, firma):
                    insertar_ot(conn_sqlite, cursor, datos['activity_id'], datos['OT'])
                
                # Preparar datos para el JSON que se enviará al endpoint
                comentarios_nuevos_para_envio.append({
                    "ID": comment_id, "ACTIVITY_ID": datos['activity_id'],
                    "OT": datos['OT'], "ACTIVITY_NAME": datos['activity_name'],
                    "ROLE_NAME": datos['role_name'], "WORK_SEQUENCE_NAME": datos['work_sequence_name'],
                    "ELEMENT_STEP": datos['element_step'], "ELEMENT_INSTANCE_NAME": datos['element_instance_name'],
                    "SUFFIX": datos['suffix'], "COMMENT_TITLE": datos['comment_title'],
                    "COMMENT_DESCRIPTION": datos['comment_description'], "LOCATION_URLS": datos['location_urls'],
                    "COMMENT_USED_FOR": datos['comment_used_for'], "CREATED_DATE": datos['created_date'],
                    "MD5": firma
                })
            except sqlite3.IntegrityError:
                # Ya logueado en insertar_comentario.
                pass
        
        if cont_nuevos_guardados > 0:
            logger.info(f"Total de comentarios nuevos guardados en modo temp: {cont_nuevos_guardados}")
        
        return comentarios_nuevos_para_envio

    except Exception:
        logger.exception("Error crítico en 'crear_comentarios_temp'.")
        return []


def crear_comentarios(session, query, conn_sqlite, parametro):
    """Función dispatcher que llama al modo correcto según el parámetro."""
    logger.info(f"Iniciando subproceso: Sincronización de Comentarios en modo '{parametro.upper()}'.")
    if parametro == "historico":
        crear_comentarios_historico(session, query, conn_sqlite)
        return None
    elif parametro == "temp":
        return crear_comentarios_temp(session, query, conn_sqlite)
    else:
        # Este error no debería ocurrir si se valida en main.py, pero es una salvaguarda.
        msg = f"Parámetro de modo de creación de comentarios no reconocido: '{parametro}'"
        logger.error(msg)
        raise ValueError(msg)