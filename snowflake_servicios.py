# -*- coding: utf-8 -*-
"""
Este módulo proporciona servicios para interactuar con Snowflake y una base de datos local SQLite.
Incluye funcionalidades para registrar eventos, descargar imágenes, generar hashes MD5 y procesar datos
de Snowflake para almacenarlos localmente.
"""

import os
import sqlite3
import hashlib
import ast
import webbrowser,pyautogui,time
from urllib.parse import urlparse
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import base64
from carga_servicios import jsonTemp
import json
from datetime import datetime

def log():
    """
    Inicializa el archivo de log (Log.txt), sobrescribiéndolo si ya existe.
    """
    with open("Log.txt","w",encoding="utf-8") as archivo:
        archivo.write("LOG DE EJECUCIÓN Y DE ERRORES\n")

def agregarEnLog(texto):
    """
    Agrega una línea de texto al archivo de log.

    Args:
        texto (str): El texto que se agregará al log.
    """
    with open("Log.txt","a",encoding="utf-8") as archivo:
        archivo.write(texto + "\n")

def descarga_img_selenium(url, contImg, ID):
    """
    Descarga una imagen desde una URL utilizando Selenium con un navegador en modo headless.
    La imagen se guarda en la carpeta 'carpeta_imagenes' con un nombre único.

    Args:
        url (str): La URL de la página que contiene la imagen.
        contImg (int): Un contador para el nombre del archivo de imagen.
        ID (int): El identificador único asociado a la imagen.

    Returns:
        bool: True si la imagen se descargó correctamente, False en caso contrario.
    """
    # Configura las opciones del navegador para que no se abra una ventana (modo headless)
    options = webdriver.EdgeOptions()
    options.add_argument("--headless=new")
    driver = webdriver.Edge(options=options)
 
    try:
        driver.get(url)
 
        # Espera a que la imagen esté presente en la página
        wait = WebDriverWait(driver, 20)
        img = wait.until(EC.presence_of_element_located((By.TAG_NAME, "img")))
 
        src = img.get_attribute("src")

        # Script de JavaScript para obtener la imagen en formato base64
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
 
        # Ejecuta el script de JavaScript en el navegador
        base64_data = driver.execute_script(js, src)  
 
        if not base64_data:
            print("No se pudo descargar la imagen con JS")
            return False
 
        # Convierte los datos de base64 a bytes
        imagen = base64.b64decode(base64_data)
 
        # Guarda la imagen en un archivo
        nombre = f"{ID}_{contImg}.jpg"
        ruta_destino = os.path.join("carpeta_imagenes", nombre)
 
        with open(ruta_destino, "wb") as f:
            f.write(imagen)
 
        print("Imagen descargada OK:", ruta_destino)
        return True
 
    except Exception as e:
        print("Error descargando:", e)
        agregarEnLog("### ERROR ###")
        agregarEnLog(f"Error procesando: {e}")
        return False
 
    finally:
        driver.quit()

def generar_md5(*valores):
    """
    Genera un hash MD5 a partir de una serie de valores.

    Args:
        *valores: Una secuencia de valores para concatenar y generar el hash.

    Returns:
        str: El hash MD5 en formato hexadecimal.
    """
    cadena = "|".join(str(v) for v in valores)
    return hashlib.md5(cadena.encode("utf-8")).hexdigest()

def crear_ot(session, queryInicio, connSqlite):
    """
    Ejecuta una consulta en Snowflake para obtener órdenes de trabajo (OT) y las guarda en una tabla SQLite local.
    Utiliza un hash MD5 para evitar duplicados.

    Args:
        session: La sesión activa de Snowflake.
        queryInicio (str): La consulta SQL para obtener las OT de Snowflake.
        connSqlite: La conexión a la base de datos SQLite.
    """
    try:
        cont = 0
        cursor = connSqlite.cursor()

        # Crea la tabla 'ot_lista' en SQLite si no existe
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ot_lista (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ACTIVITY_ID INTEGER,
            SAP_WORK_NUMBER TEXT,
            MD5 TEXT UNIQUE
        )
        """)
        print("Conexion correcta tabla ot_lista sqlite")

        # Ejecuta la consulta en Snowflake
        ot = session.sql(queryInicio)
        rows1 = ot.collect()
        print("Se hace consulta de lista de ot a snowflake")

        # Itera sobre los resultados de Snowflake e inserta en SQLite
        for row in rows1:
            acID = row["ACTIVITY_ID"]
            SWN = row["SAP_WORK_NUMBER"]
            firma = generar_md5(acID, SWN)

            try:
                cursor.execute("""
                    INSERT INTO ot_lista(ACTIVITY_ID, SAP_WORK_NUMBER, MD5) values (?,?,?)
                    """, (acID, SWN, firma))
                cont += 1
            except sqlite3.IntegrityError:
                # Si el MD5 ya existe, no se inserta el duplicado
                pass
        print("Se guardaron las ot exitosamentes en sqlite!")
        print("Cantidad de ot : ", cont)

    except Exception as e:
        print(f"Error al conectar o ejecutar la consulta: {e}")
        agregarEnLog("### ERROR ###")
        agregarEnLog(f"Error procesando: {e}")

def crear_comentarios(session, query, connSqlite, parametro):
    """
    Procesa los comentarios de Snowflake, los guarda en SQLite y descarga las imágenes asociadas.
    Puede operar en modo 'historico' (procesa todo) o 'temp' (procesa solo lo nuevo).

    Args:
        session: La sesión activa de Snowflake.
        query (str): La consulta SQL para obtener los comentarios de Snowflake.
        connSqlite: La conexión a la base de datos SQLite.
        parametro (str): El modo de operación ('historico' o 'temp').
    """
    cont = 0
    contImgTotal = 0
    comentarios_nuevos = []

    try:
        # Crea la carpeta para las imágenes si no existe
        os.makedirs("carpeta_imagenes", exist_ok=True)
        print("Carpeta carpeta_imagenes creada!")

        cursor = connSqlite.cursor()

        # Crea la tabla 'comentarios' en SQLite si no existe
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
            MD5 TEXT
        )
        """)
        print("Se crea tabla de comentarios en sqlite!")

        nuevas_firmas = []

        # Ejecuta la consulta en Snowflake
        comments = session.sql(query)
        rowsComents = comments.collect()

        contComments = 0
        contCommentImgs = 0

        # Cuenta el total de comentarios e imágenes a procesar
        for row in rowsComents:
            contComments += 1
            LU = row["LOCATION_URLS"]
            if LU:
                try:
                    urls = ast.literal_eval(LU)
                    for url in urls:
                        if url:
                            contCommentImgs += 1
                except Exception as e:
                    print("Error procesando imágenes:", e)
                        
        agregarEnLog("### CANTIDAD DE COMENTARIOS E IMAGENES QUE VIENEN EN TOTAL ###")
        agregarEnLog(f"Cantidad de comentarios : {contComments} ; Cantidad de imagenes : {contCommentImgs}")

        # Procesa cada comentario
        for row in rowsComents:
            ID = row["ID"]
            acID = row["ACTIVITY_ID"]
            SWN = row["SAP_WORK_NUMBER"]
            RN = row["ROLE_NAME"]
            WSN = row["WORK_SEQUENCE_NAME"]
            ES = row["ELEMENT_STEP"]
            EIN = row["ELEMENT_INSTANCE_NAME"]
            Suffix = row["SUFFIX"]
            CT = row["COMMENT_TITLE"]
            CD = row["COMMENT_DESCRIPTION"]
            LU = row["LOCATION_URLS"]
            CUF = row["COMMENT_USED_FOR"]
            CDate = row["CREATED_DATE"]
            firma = generar_md5(acID, SWN)

            cursor.execute("SELECT 1 FROM ot_lista WHERE MD5 = ?", (firma,))
            existe = cursor.fetchone()

            # Descarga de imágenes
            if parametro == "historico":
                contImg = 1
                if LU:
                    try:
                        urls = ast.literal_eval(LU)
                        for url in urls:
                            if url:
                                descarga_img_selenium(url, contImg, ID)
                                contImg += 1
                                contImgTotal += 1
                    except Exception as e:
                        print("Error procesando imágenes:", e)
                        agregarEnLog("### ERROR ###")
                        agregarEnLog(f"Error procesando: {e}")
            
            elif parametro == "temp" and not existe:
                contImg = 1
                if LU:
                    try:
                        urls = ast.literal_eval(LU)
                        for url in urls:
                            if url:
                                descarga_img_selenium(url, contImg, ID)
                                contImg += 1
                                contImgTotal += 1
                    except Exception as e:
                        print("Error procesando imágenes:", e)
                        agregarEnLog("### ERROR ###")
                        agregarEnLog(f"Error procesando: {e}")

            # Inserción en la base de datos SQLite
            try:
                if parametro == "historico":
                    cursor.execute("""
                        INSERT INTO comentarios(ID,ACTIVITY_ID,SAP_WORK_NUMBER,ROLE_NAME,WORK_SEQUENCE_NAME,ELEMENT_STEP,ELEMENT_INSTANCE_NAME,SUFFIX,COMMENT_TITLE,COMMENT_DESCRIPTION,LOCATION_URLS,COMMENT_USED_FOR,CREATED_DATE,MD5) 
                        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (ID,acID,SWN,RN,WSN,ES,EIN,Suffix,CT,CD,LU,CUF,CDate,firma))
                    cont += 1
                elif parametro == "temp" and not existe:
                    cursor.execute("""
                        INSERT INTO comentarios(ID,ACTIVITY_ID,SAP_WORK_NUMBER,ROLE_NAME,WORK_SEQUENCE_NAME,ELEMENT_STEP,ELEMENT_INSTANCE_NAME,SUFFIX,COMMENT_TITLE,COMMENT_DESCRIPTION,LOCATION_URLS,COMMENT_USED_FOR,CREATED_DATE,MD5) 
                        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (ID,acID,SWN,RN,WSN,ES,EIN,Suffix,CT,CD,LU,CUF,CDate,firma))
                    cont += 1
                    nuevas_firmas.append((acID, SWN, firma))
                    comentarios_nuevos.append({
                        "ID": ID, "ACTIVITY_ID": acID, "SAP_WORK_NUMBER": SWN, "ROLE_NAME": RN,
                        "WORK_SEQUENCE_NAME": WSN, "ELEMENT_STEP": ES, "ELEMENT_INSTANCE_NAME": EIN,
                        "SUFFIX": Suffix, "COMMENT_TITLE": CT, "COMMENT_DESCRIPTION": CD,
                        "LOCATION_URLS": LU, "COMMENT_USED_FOR": CUF, "CREATED_DATE": CDate, "MD5": firma
                    })
            except sqlite3.IntegrityError:
                pass
        
        # Guarda las nuevas OT en 'ot_lista' si es un proceso temporal
        if parametro == "temp":
            for acID, SWN, firma in nuevas_firmas:
                try:
                    cursor.execute("""
                    insert into ot_lista(ACTIVITY_ID,SAP_WORK_NUMBER,MD5)
                    VALUES (?,?,?)
                    """, (acID, SWN, firma))
                except sqlite3.IntegrityError:
                    pass
        
        # Crea un JSON temporal con los nuevos comentarios
        if parametro == "temp" and comentarios_nuevos:
            fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            nombre = f"comentarios_temp_{fecha}.json"
    
            with open(nombre, "w", encoding="utf-8") as f:
                json.dump(comentarios_nuevos, f, ensure_ascii=False, indent=4)
    
            print(f"JSON temporal creado con {len(comentarios_nuevos)} comentarios nuevos: {nombre}")
    
        print("Cantidad de comentarios procesados : ", cont)
        print("Cantidad de imagenes procesadas : ", contImgTotal)
    except Exception as e:
        print(f"Error al conectar o ejecutar la consulta: {e}")
        agregarEnLog("### ERROR ###")
        agregarEnLog(f"Error procesando: {e}")
        