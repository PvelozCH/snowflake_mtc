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


ENDPOINT = "http://localhost:8000/recibir-json"


# ============================================================================
# FUNCIONES DE UTILIDAD GENERAL
# ============================================================================

def log():
    """Inicializa el archivo de log sobrescribiendo el existente"""
    with open("Log.txt", "w", encoding="utf-8") as archivo:
        archivo.write("LOG DE EJECUCIÓN Y DE ERRORES\n")


def agregarEnLog(texto):
    """Agrega una línea al archivo de log"""
    with open("Log.txt", "a", encoding="utf-8") as archivo:
        archivo.write(texto + "\n")


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

    # Verificar si la imagen ya existe para no duplicarla
    if os.path.exists(ruta_destino):
        print(f"Imagen ya existe, se omite la descarga: {ruta_destino}")
        return ruta_destino

    options = webdriver.EdgeOptions()
    options.add_argument("--headless=new")
    driver = webdriver.Edge(options=options)
    
    try:
        driver.get(url)
        
        # Esperar a que la imagen cargue
        wait = WebDriverWait(driver, 20)
        img = wait.until(EC.presence_of_element_located((By.TAG_NAME, "img")))
        
        src = img.get_attribute("src")
        
        # Script JS para obtener la imagen en base64
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
            print("No se pudo descargar la imagen con JS")
            return None
        
        # Decodificar y guardar la imagen
        imagen = base64.b64decode(base64_data)
        
        with open(ruta_destino, "wb") as f:
            f.write(imagen)
        
        print("Imagen descargada OK:", ruta_destino)
        return ruta_destino
    
    except Exception as e:
        print("Error descargando:", e)
        agregarEnLog("### ERROR ###")
        agregarEnLog(f"Error procesando: {e}")
        return None
    
    finally:
        driver.quit()


# ============================================================================
# FUNCIONES DE PROCESAMIENTO DE IMÁGENES (POR MODO)
# ============================================================================

def procesar_imagenes_historico(location_urls, comment_id):
    """
    Procesa imágenes en modo HISTÓRICO:
    - Solo descarga y guarda las imágenes localmente
    - No envía al endpoint (se envían todas al final)
    """
    if not location_urls:
        return 0
    
    contador_imagenes = 0
    
    try:
        urls = ast.literal_eval(location_urls)
        cont_img = 1
        
        for url in urls:
            if not url:
                continue
            
            print(f"    -> Descargando imagen {cont_img} para comentario {comment_id}...")
            ruta_img = descarga_img_selenium(url, cont_img, comment_id)
            
            if ruta_img:
                cont_img += 1
                contador_imagenes += 1
    
    except Exception as e:
        print(f"Error procesando imágenes para comentario {comment_id}: {e}")
        agregarEnLog(f"### ERROR ###\nError procesando imágenes para comentario {comment_id}: {e}")
    
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
        SAP_WORK_NUMBER TEXT,
        MD5 TEXT UNIQUE
    )
    """)
    print("Conexion correcta tabla ot_lista sqlite")


def crear_tabla_comentarios(cursor):
    """Crea la tabla de comentarios si no existe"""
    # CORREGIDO: Se añade la columna ACTIVITY_NAME en la definición de la tabla.
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comentarios (
        id INTEGER PRIMARY KEY,
        ACTIVITY_ID INTEGER,
        SAP_WORK_NUMBER TEXT,
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
    print("Se crea tabla de comentarios en sqlite!")


# ============================================================================
# FUNCIONES DE INSERCIÓN Y CONSULTA EN SQLITE
# ============================================================================

def insertar_ot(conn_sqlite, cursor, activity_id, sap_work_number):
    """
    Inserta una orden de trabajo en SQLite y guarda el cambio inmediatamente.
    Retorna True si se insertó, False si ya existía (por MD5 duplicado).
    """
    firma = generar_md5(activity_id, sap_work_number)
    
    try:
        cursor.execute("""
            INSERT INTO ot_lista(ACTIVITY_ID, SAP_WORK_NUMBER, MD5) 
            VALUES (?,?,?)
        """, (activity_id, sap_work_number, firma))
        conn_sqlite.commit()
        print(f"  -> Nueva OT guardada en SQLite: {activity_id} / {sap_work_number}")
        return True
    except sqlite3.IntegrityError:
        return False


def insertar_comentario(conn_sqlite, cursor, datos_comentario):
    """Inserta un comentario completo en SQLite y guarda el cambio inmediatamente."""
    # CORREGIDO: Se añade ACTIVITY_NAME a la lista de columnas. El orden ahora es el correcto.
    cursor.execute("""
        INSERT INTO comentarios(
            ID, ACTIVITY_ID, SAP_WORK_NUMBER, ROLE_NAME, WORK_SEQUENCE_NAME,
            ELEMENT_STEP, ELEMENT_INSTANCE_NAME, SUFFIX, COMMENT_TITLE,
            COMMENT_DESCRIPTION, LOCATION_URLS, COMMENT_USED_FOR, CREATED_DATE,
            MD5, ACTIVITY_NAME
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, datos_comentario)
    conn_sqlite.commit()


def comentario_existe(cursor, comment_id):
    """Verifica si un comentario ya existe en SQLite por su ID"""
    cursor.execute("SELECT 1 FROM comentarios WHERE ID = ?", (comment_id,))
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
        cursor.execute("SELECT * FROM comentarios WHERE status = 'pendiente'")
        rows = cursor.fetchall()
        comentarios = [dict(row) for row in rows]
        print(f"Se encontraron {len(comentarios)} comentarios 'pendientes' para procesar.")
        return comentarios
    finally:
        conn_sqlite.row_factory = None

def update_status_exitoso(conn_sqlite, comentarios):
    """Actualiza el estado de una lista de comentarios a 'exitoso'."""
    if not comentarios:
        return
    
    comment_ids = [c['ID'] for c in comentarios]
    print(f"Actualizando estado a 'exitoso' para {len(comment_ids)} comentarios...")
    
    cursor = conn_sqlite.cursor()
    
    placeholders = ','.join('?' for _ in comment_ids)
    query = f"UPDATE comentarios SET status = 'exitoso' WHERE ID IN ({placeholders})"
    
    cursor.execute(query, comment_ids)
    print(f"  -> {cursor.rowcount} comentarios actualizados exitosamente.")


# ============================================================================
# FUNCIONES DE TRANSFORMACIÓN DE DATOS
# ============================================================================

def extraer_datos_comentario(row):
    """
    Extrae los datos de un row de Snowflake y los convierte en un diccionario limpio
    """
    # CORREGIDO: Se asegura que cada campo del diccionario corresponde a la columna correcta de Snowflake.
    return {
        'comment_id': row["ID"],
        'activity_id': row["ACTIVITY_ID"],
        'sap_work_number': row["SAP_WORK_NUMBER"],
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
    """
    Prepara los datos para inserción en SQLite
    Retorna: (tupla_para_insert, firma_md5)
    """
    # Esta función no se toca, debe seguir generando la firma con el ID.
    firma = generar_md5(datos['activity_id'], datos['sap_work_number'])
    
    # CORREGIDO: El orden de los valores en esta tupla ahora coincide EXACTAMENTE
    # con el orden de las columnas en la sentencia INSERT de la función insertar_comentario.
    return (
        datos['comment_id'],
        datos['activity_id'],
        datos['sap_work_number'],
        datos['role_name'],
        datos['work_sequence_name'],
        datos['element_step'],
        datos['element_instance_name'],
        datos['suffix'],
        datos['comment_title'],
        datos['comment_description'],
        datos['location_urls'],
        datos['comment_used_for'],
        datos['created_date'],
        firma, # El MD5 va en la posición 14
        datos["activity_name"] # El nuevo campo va en la posición 15
    ), firma

def crear_json_temporal(comentarios_nuevos):
    """
    Crea un archivo JSON temporal con los comentarios nuevos
    Retorna el nombre del archivo creado o None si no hay comentarios
    """
    if not comentarios_nuevos:
        return None
    
    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nombre = f"comentarios_temp_{fecha}.json"
    
    with open(nombre, "w", encoding="utf-8") as f:
        json.dump(comentarios_nuevos, f, ensure_ascii=False, indent=4)
    
    print(f"Se encontraron {len(comentarios_nuevos)} comentarios nuevos")
    return nombre


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def contar_imagenes_totales(rows_comments):
    """Cuenta el total de imágenes en todos los comentarios"""
    cont_imagenes = 0
    for row in rows_comments:
        location_urls = row["LOCATION_URLS"]
        if location_urls:
            try:
                urls = ast.literal_eval(location_urls)
                cont_imagenes += len([url for url in urls if url])
            except:
                pass
    return cont_imagenes


# ============================================================================
# FUNCIÓN PRINCIPAL: CREAR ÓRDENES DE TRABAJO
# ============================================================================

def crear_ot(session, query_inicio, conn_sqlite):
    """
    Obtiene las órdenes de trabajo desde Snowflake y las guarda en SQLite
    Evita duplicados usando hash MD5 y guarda cada una individualmente.
    """
    try:
        cursor = conn_sqlite.cursor()
        crear_tabla_ot(cursor)
        
        ot = session.sql(query_inicio)
        rows = ot.collect()
        print(f"### OBTENIENDO OTs: {len(rows)} OTs recibidas de Snowflake ###")
        
        cont = 0
        for row in rows:
            ac_id = row["ACTIVITY_ID"]
            swn = row["SAP_WORK_NUMBER"]
            
            if insertar_ot(conn_sqlite, cursor, ac_id, swn):
                cont += 1
        
        print(f"Total de OTs nuevas guardadas en esta ejecución: {cont}")
    
    except Exception as e:
        print(f"Error al conectar o ejecutar la consulta en crear_ot: {e}")
        agregarEnLog(f"### ERROR ###\nError en crear_ot: {e}")


# ============================================================================
# FUNCIÓN PRINCIPAL: CREAR COMENTARIOS MODO HISTÓRICO
# ============================================================================

def crear_comentarios_historico(session, query, conn_sqlite):
    """
    Procesa comentarios en modo HISTÓRICO:
    - Obtiene todos los comentarios desde Snowflake, los guarda en SQLite y descarga sus imágenes.
    """
    cont_imagenes_total = 0
    try:
        os.makedirs("carpeta_imagenes", exist_ok=True)
        cursor = conn_sqlite.cursor()
        crear_tabla_comentarios(cursor)
        comments = session.sql(query)
        rows_comments = comments.collect()
        
        print(f"### MODO HISTORICO: {len(rows_comments)} comentarios recibidos de Snowflake ###")
        cont_nuevos = 0
        for row in rows_comments:
            datos = extraer_datos_comentario(row)
            if comentario_existe(cursor, datos['comment_id']):
                continue
            
            datos_insercion, _ = preparar_datos_insercion(datos)
            try:
                insertar_comentario(conn_sqlite, cursor, datos_insercion)
                
                cont_nuevos += 1
                cont_imagenes_total += procesar_imagenes_historico(
                    datos['location_urls'], 
                    datos['comment_id']
                )
            except sqlite3.IntegrityError as e:
                print(f"Error de integridad al insertar comentario ID={datos['comment_id']}: {e}")
        
        print(f"Total de comentarios nuevos guardados en esta ejecución: {cont_nuevos}")
        print(f"Total de imágenes descargadas en esta ejecución: {cont_imagenes_total}")

    except Exception as e:
        print(f"Error en crear_comentarios_historico: {e}")
        agregarEnLog(f"### ERROR ###\nError en crear_comentarios_historico: {e}")

def crear_comentarios_temp(session, query, conn_sqlite):
    """
    Procesa comentarios en modo TEMPORAL:
    - Identifica y guarda nuevos comentarios en SQLite.
    - Descarga localmente las imágenes de los nuevos comentarios.
    - Retorna una lista con los datos de los comentarios nuevos.
    """
    comentarios_nuevos = []
    cont_nuevos_guardados = 0
    try:
        os.makedirs("carpeta_imagenes", exist_ok=True)
        cursor = conn_sqlite.cursor()
        crear_tabla_comentarios(cursor)
        comments = session.sql(query)
        rows_comments = comments.collect()
        print(f"### MODO TEMP: {len(rows_comments)} comentarios recibidos de Snowflake ###")

        for row in rows_comments:
            datos = extraer_datos_comentario(row)
            if comentario_existe(cursor, datos['comment_id']):
                continue
            
            datos_insercion, firma = preparar_datos_insercion(datos)
            try:
                insertar_comentario(conn_sqlite, cursor, datos_insercion)
                cont_nuevos_guardados += 1

                procesar_imagenes_historico(
                    datos['location_urls'], 
                    datos['comment_id']
                )
                
                if not ot_existe(cursor, firma):
                    insertar_ot(conn_sqlite, cursor, datos['activity_id'], datos['sap_work_number'])
                
                # CORREGIDO: Se añade el nuevo campo activity_name al diccionario para el JSON temporal.
                comentarios_nuevos.append({
                    "ID": datos['comment_id'], 
                    "ACTIVITY_ID": datos['activity_id'],
                    "SAP_WORK_NUMBER": datos['sap_work_number'], 
                    "ACTIVITY_NAME": datos['activity_name'],
                    "ROLE_NAME": datos['role_name'],
                    "WORK_SEQUENCE_NAME": datos['work_sequence_name'], 
                    "ELEMENT_STEP": datos['element_step'],
                    "ELEMENT_INSTANCE_NAME": datos['element_instance_name'], 
                    "SUFFIX": datos['suffix'],
                    "COMMENT_TITLE": datos['comment_title'], 
                    "COMMENT_DESCRIPTION": datos['comment_description'],
                    "LOCATION_URLS": datos['location_urls'], 
                    "COMMENT_USED_FOR": datos['comment_used_for'],
                    "CREATED_DATE": datos['created_date'], 
                    "MD5": firma
                })
            except sqlite3.IntegrityError as e:
                print(f"Error de integridad al insertar comentario ID={datos['comment_id']}: {e}")
        
        if cont_nuevos_guardados > 0:
            print(f"Total de comentarios nuevos guardados en esta ejecución: {cont_nuevos_guardados}")

        return comentarios_nuevos
    except Exception as e:
        print(f"Error en crear_comentarios_temp: {e}")
        agregarEnLog(f"### ERROR ###\nError en crear_comentarios_temp: {e}")
        return []

def crear_comentarios(session, query, conn_sqlite, parametro):
    """
    Función dispatcher que llama al modo correcto según el parámetro.
    - 'historico': Carga todo y no devuelve nada.
    - 'temp': Carga solo lo nuevo y devuelve una lista de los nuevos comentarios.
    """
    if parametro == "historico":
        crear_comentarios_historico(session, query, conn_sqlite)
        return None
    elif parametro == "temp":
        return crear_comentarios_temp(session, query, conn_sqlite)
    else:
        raise ValueError(f"Parámetro '{parametro}' no reconocido")
