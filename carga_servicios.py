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

DB_PATH = "BDD_SNOWFLAKE.db"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def _crear_sesion_con_reintentos():
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
    return session

session = _crear_sesion_con_reintentos()

def agregarEnLog(texto):
    """Agrega una línea al archivo de log"""
    with open("Log.txt", "a", encoding="utf-8") as archivo:
        archivo.write(texto + "\n")


def _get_auth_headers():

    headers = {
        "Content-Type": "application/json"
    }

    token = os.environ.get("ENDPOINT_BEARER_TOKEN")
    headers["Authorization"] = f"Bearer gHoVErMX09mm05Rzdzbh1g1gKJn9gFAZafsmUtvSEeXQtOCQfJr3Amow0y3Z"
    print(f"headers: {headers}")
    return headers


# ============================================================================
# FUNCIONES DE ENVÍO A ENDPOINT
# ============================================================================

def cargaEndpoint(ruta_json, endpoint):
    """
    Carga un archivo JSON completo a un endpoint
    Usado para enviar JSONs de comentarios (histórico o temporal)
    """
    try:
        if not os.path.isfile(ruta_json):
            raise FileNotFoundError(f"El archivo {ruta_json} no existe")
        
        with open(ruta_json, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        headers = _get_auth_headers()
        
        response = session.post(endpoint, json=data, timeout=180, headers=headers, verify=False)
        response.raise_for_status()
        
        print(f"Envío de archivo JSON exitoso: {ruta_json}")
    
    except Exception as e:
        print(f"Error enviando JSON: {e}")
        agregarEnLog(f"### ERROR EN ENDPOINT (JSON) ###\n{e}")
        raise e


def enviar_imagen_json_memoria(ruta_imagen, tipo, endpoint, timeout=360):
    """
    Convierte una imagen a Base64 y la envía como JSON al endpoint
    Usado en modo temp para enviar imágenes inmediatamente
    Levanta una excepción si falla.
    """
    try:
        with open(ruta_imagen, "rb") as f:
            imagen_b64 = base64.b64encode(f.read()).decode("utf-8")
        
        nombre = os.path.basename(ruta_imagen)
        
        # Extraer ID del comentario del nombre del archivo (formato: ID_1.jpg)
        comment_id = nombre.split("_")[0]
        
        payload = {
            "comment_id": comment_id,
            "filename": nombre,
            "tipo": tipo,
            "imagen_b64": imagen_b64
        }


        headers = _get_auth_headers()
        response = session.post(endpoint, json=payload, timeout=timeout,headers=headers,verify=False)
        response.raise_for_status()
    
    except Exception as e:
        print(f"Error enviando imagen: {e}")
        agregarEnLog(f"### ERROR EN ENDPOINT (IMAGEN) ###\n{e}")
        raise e


def enviar_carpeta_imagenes_memoria(carpeta_imagenes, tipo, endpoint):
    """
    Recorre una carpeta y envía cada imagen individualmente como JSON
    Usado en modo histórico para enviar todas las imágenes al final
    """
    if not os.path.isdir(carpeta_imagenes):
        raise FileNotFoundError(f"No existe la carpeta: {carpeta_imagenes}")
    
    for nombre in sorted(os.listdir(carpeta_imagenes)):
        ruta = os.path.join(carpeta_imagenes, nombre)
        
        if not os.path.isfile(ruta):
            continue
        
        enviar_imagen_json_memoria(
            ruta_imagen=ruta,
            tipo=tipo,
            endpoint=endpoint
        )
        print(f"Imagen enviada: {nombre}")
        
        

# ============================================================================
# FUNCIONES DE SERIALIZACIÓN
# ============================================================================

def serializar_fechas(obj):
    """Convierte objetos datetime y date a formato ISO 8601 para JSON"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


# ============================================================================
# FUNCIONES DE GENERACIÓN DE JSON
# ============================================================================

def jsonOt():
    """
    Genera archivo JSON con la lista de órdenes de trabajo desde SQLite
    Archivo generado: 1.ot_lista.json
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM ot_lista")
        rows = cursor.fetchall()
        
        data_ot = [dict(row) for row in rows]
        
        with open("1.ot_lista.json", "w", encoding="utf-8") as f:
            json.dump(data_ot, f, ensure_ascii=False, indent=4)
        
        print("Se creó JSON de OT")
        conn.close()
    
    except Exception as e:
        print(f"Error generando JSON de OT: {e}")


def jsonHistorico():
    """
    Genera archivo JSON con todos los comentarios históricos desde SQLite
    Archivo generado: 2.comentarios_por_ot_historico.json
    Convierte fechas a formato ISO para compatibilidad JSON
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM comentarios")
        rows = cursor.fetchall()
        
        data = []
        
        # Procesar cada comentario y serializar fechas
        for row in rows:
            d = dict(row)
            
            for k, v in d.items():
                d[k] = serializar_fechas(v)
            
            data.append(d)
        
        with open("2.comentarios_por_ot_historico.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        print("Se creó JSON de comentarios históricos")
        conn.close()
    
    except Exception as e:
        print(f"Error generando JSON histórico: {e}")