"""
Servicios para generación de JSON y envío a endpoints
Maneja la conversión de datos SQLite a JSON y el envío de archivos/imágenes
"""
import sqlite3
import json
import os
from datetime import datetime, date
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import base64
import urllib3

from logger_config import logger

DB_PATH = "BDD_SNOWFLAKE.db"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def _crear_sesion_con_reintentos():
    """Crea una sesión de requests con reintentos automáticos para errores de servidor."""
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504)
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    logger.info("Sesión de requests con estrategia de reintentos creada.")
    return session

session = _crear_sesion_con_reintentos()

def _get_auth_headers():
    """Obtiene los encabezados de autenticación, priorizando el token de entorno."""
    headers = {"Content-Type": "application/json"}
    
    token = os.environ.get("ENDPOINT_BEARER_TOKEN")
    if token and token != "tu_token_secreto_aqui":
        logger.info("Usando token de autenticación desde la variable de entorno ENDPOINT_BEARER_TOKEN.")
        headers["Authorization"] = f"Bearer {token}"
    else:
        hardcoded_token = "gHoVErMX09mm05Rzdzbh1g1gKJn9gFAZafsmUtvSEeXQtOCQfJr3Amow0y3Z"
        logger.warning("La variable de entorno ENDPOINT_BEARER_TOKEN no está definida o contiene el valor por defecto. Usando token harcodeado.")
        headers["Authorization"] = f"Bearer {hardcoded_token}"
        
    return headers


# ============================================================================
# FUNCIONES DE ENVÍO A ENDPOINT
# ============================================================================

def cargaEndpoint(ruta_json, endpoint):
    """
    Carga un archivo JSON completo a un endpoint. Levanta excepción si falla.
    """
    logger.info(f"Iniciando envío de archivo JSON '{ruta_json}' al endpoint: {endpoint}")
    try:
        if not os.path.isfile(ruta_json):
            logger.error(f"El archivo JSON especificado no existe: {ruta_json}")
            raise FileNotFoundError(f"El archivo {ruta_json} no existe")
        
        with open(ruta_json, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        headers = _get_auth_headers()
        
        response = session.post(endpoint, json=data, timeout=180, headers=headers, verify=False)
        response.raise_for_status()
        
        logger.info(f"Envío de '{ruta_json}' al endpoint {endpoint} completado exitosamente (Status: {response.status_code}).")
    
    except Exception as e:
        logger.exception(f"Error durante el envío del archivo JSON '{ruta_json}' al endpoint {endpoint}.")
        raise e


def enviar_imagen_json_memoria(ruta_imagen, tipo, endpoint, timeout=360):
    """
    Convierte una imagen a Base64 y la envía como JSON al endpoint. Levanta excepción si falla.
    """
    nombre_img = os.path.basename(ruta_imagen)
    logger.info(f"Procesando imagen '{nombre_img}' para envío al endpoint: {endpoint}")
    try:
        with open(ruta_imagen, "rb") as f:
            imagen_b64 = base64.b64encode(f.read()).decode("utf-8")
        
        comment_id = nombre_img.split("_")[0]
        
        payload = {
            "comment_id": comment_id,
            "filename": nombre_img,
            "tipo": tipo,
            "imagen_b64": imagen_b64
        }

        headers = _get_auth_headers()
        response = session.post(endpoint, json=payload, timeout=timeout, headers=headers, verify=False)
        response.raise_for_status()
        logger.info(f"Imagen '{nombre_img}' enviada exitosamente al endpoint {endpoint} (Status: {response.status_code}).")
    
    except Exception as e:
        logger.exception(f"Error enviando la imagen '{nombre_img}' al endpoint {endpoint}.")
        raise e


def enviar_carpeta_imagenes_memoria(carpeta_imagenes, tipo, endpoint):
    """
    Recorre una carpeta y envía cada imagen individualmente como JSON.
    """
    logger.info(f"Iniciando envío de todas las imágenes desde la carpeta '{carpeta_imagenes}'...")
    if not os.path.isdir(carpeta_imagenes):
        logger.error(f"La carpeta de imágenes especificada no existe: {carpeta_imagenes}")
        raise FileNotFoundError(f"No existe la carpeta: {carpeta_imagenes}")
    
    archivos_en_carpeta = sorted(os.listdir(carpeta_imagenes))
    logger.info(f"Se encontraron {len(archivos_en_carpeta)} archivos en la carpeta.")

    for nombre in archivos_en_carpeta:
        ruta = os.path.join(carpeta_imagenes, nombre)
        
        if not os.path.isfile(ruta):
            logger.warning(f"Se encontró un elemento que no es un archivo y será omitido: {ruta}")
            continue
        

        enviar_imagen_json_memoria(
            ruta_imagen=ruta,
            tipo=tipo,
            endpoint=endpoint
        )
    logger.info(f"Proceso de envío de imágenes desde la carpeta '{carpeta_imagenes}' finalizado.")


def enviar_imagenes_de_comentario(comment_id, carpeta_imagenes, tipo, endpoint):
    """
    Busca y envía todas las imágenes asociadas a un comment_id.
    Retorna True si todas las imágenes se envían con éxito o si no hay imágenes.
    Retorna False si falla el envío de alguna imagen.
    """
    logger.info(f"Buscando imágenes para el comentario ID {comment_id} en '{carpeta_imagenes}'...")
    
    if not os.path.isdir(carpeta_imagenes):
        logger.error(f"La carpeta de imágenes '{carpeta_imagenes}' no existe. No se pueden enviar imágenes.")
        return True # Si la carpeta no existe, no hay imágenes que enviar, se considera éxito para no bloquear el comentario.

    try:
        # Convert comment_id to string to be safe
        comment_id_str = str(comment_id)
        imagenes_a_enviar = [
            os.path.join(carpeta_imagenes, f)
            for f in sorted(os.listdir(carpeta_imagenes))
            if f.startswith(f"{comment_id_str}_") and os.path.isfile(os.path.join(carpeta_imagenes, f))
        ]
    except Exception as e:
        logger.exception(f"Error al buscar imágenes para el comment_id {comment_id} en {carpeta_imagenes}")
        return False


    if not imagenes_a_enviar:
        logger.info(f"No se encontraron imágenes para el comentario ID {comment_id}.")
        return True

    logger.info(f"Se encontraron {len(imagenes_a_enviar)} imágenes para el comentario ID {comment_id}. Iniciando envío...")
    
    for i, ruta_imagen in enumerate(imagenes_a_enviar):
        try:
            logger.info(f"Enviando imagen {i + 1}/{len(imagenes_a_enviar)}: {os.path.basename(ruta_imagen)}")
            enviar_imagen_json_memoria(
                ruta_imagen=ruta_imagen,
                tipo=tipo,
                endpoint=endpoint
            )
        except Exception:
            logger.error(f"Fallo al enviar la imagen {os.path.basename(ruta_imagen)} para el comentario ID {comment_id}. Se cancela el resto de envíos para este comentario.")
            return False
    
    logger.info(f"Todas las {len(imagenes_a_enviar)} imágenes para el comentario ID {comment_id} fueron enviadas exitosamente.")
    return True


# ============================================================================
# FUNCIONES DE SERIALIZACIÓN
# ============================================================================

def serializar_fechas(obj):
    """Convierte objetos datetime y date a formato ISO 8601 para JSON."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


# ============================================================================
# FUNCIONES DE GENERACIÓN DE JSON
# ============================================================================

def jsonOt():
    """
    Genera archivo JSON con la lista de órdenes de trabajo desde SQLite.
    Archivo generado: 1.ot_lista.json
    """
    logger.info("Iniciando generación de JSON de Órdenes de Trabajo (OT)...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM ot_lista")
        rows = cursor.fetchall()
        
        data_ot = [dict(row) for row in rows]
        
        with open("1.ot_lista.json", "w", encoding="utf-8") as f:
            json.dump(data_ot, f, ensure_ascii=False, indent=4)
        
        logger.info(f"JSON '1.ot_lista.json' creado exitosamente con {len(data_ot)} OTs.")
        conn.close()
    
    except Exception:
        logger.exception("Error al generar el archivo JSON de OTs.")


def jsonHistorico():
    """
    Genera archivo JSON con todos los comentarios históricos desde SQLite.
    Archivo generado: 2.comentarios_por_ot_historico.json
    """
    logger.info("Iniciando generación de JSON de comentarios históricos...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM comentarios")
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            d = dict(row)
            for k, v in d.items():
                d[k] = serializar_fechas(v)
            data.append(d)
        
        with open("2.comentarios_por_ot_historico.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        logger.info(f"JSON '2.comentarios_por_ot_historico.json' creado exitosamente con {len(data)} comentarios.")
        conn.close()
    
    except Exception:
        logger.exception("Error al generar el archivo JSON de comentarios históricos.")