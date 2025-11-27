# -*- coding: utf-8 -*-
"""
Este módulo se encarga de procesar los datos almacenados en la base de datos SQLite (BDD_SNOWFLAKE.db)
y convertirlos en archivos JSON.
"""

import sqlite3
import json
import os 
import sys 
from datetime import datetime,date

def jsonOt():
    """
    Extrae los datos de la tabla 'ot_lista' de la base de datos SQLite y los guarda en un archivo JSON.
    El archivo se nombra '1.ot_lista.json'.
    """
    try:
        conn = sqlite3.connect("BDD_SNOWFLAKE.db")
        conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM ot_lista")

        rows = cursor.fetchall()

        # Convierte cada fila en un diccionario y lo agrega a una lista
        dataOt = [dict(row) for row in rows]

        # Guarda la lista de diccionarios en un archivo JSON
        with open("1.ot_lista.json","w",encoding="utf-8") as f:
            json.dump(dataOt,f,ensure_ascii=False,indent=4)
        print("Se creó json de ot")
    except Exception as e:
        print("Error : ",e)


def jsonHistorico():
    """
    Extrae todos los comentarios de la tabla 'comentarios' y los guarda en un archivo JSON.
    El archivo se nombra '2.comentarios_por_ot_historico.json'.
    Maneja la serialización de objetos de fecha y hora a formato ISO.
    """
    try:
        conn = sqlite3.connect("BDD_SNOWFLAKE.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
    
        cursor.execute("SELECT * FROM comentarios")
        rows = cursor.fetchall()
    
        data = []
    
        for row in rows:
            d = dict(row)   

            # Formatea las fechas y horas a formato ISO 8601
            for k, v in d.items():
                if isinstance(v, (datetime, date)):
                    d[k] = v.isoformat()
    
            data.append(d)
    
        # Guarda los datos en el archivo JSON
        with open("2.comentarios_por_ot_historico.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("Se creó json de comentarios históricos")
    
    except Exception as e:
        print("Error : ",e)


def jsonTemp():
    """
    Extrae todos los comentarios de la tabla 'comentarios' y los guarda en un archivo JSON.
    El archivo se nombra '2.comentarios_por_ot_historico.json'.
    NOTA: Esta función es actualmente un duplicado exacto de jsonHistorico(). 
    Debe ser revisada para asegurar que cumple el propósito deseado para los datos temporales.
    """
    try:
        conn = sqlite3.connect("BDD_SNOWFLAKE.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
    
        cursor.execute("SELECT * FROM comentarios")
        rows = cursor.fetchall()
    
        data = []
    
        for row in rows:
            d = dict(row)   

            # Formatea las fechas y horas a formato ISO 8601
            for k, v in d.items():
                if isinstance(v, (datetime, date)):
                    d[k] = v.isoformat()
    
            data.append(d)
    
        # Guarda los datos en el archivo JSON
        with open("2.comentarios_por_ot_historico.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("Se creó json de comentarios históricos")
    
    except Exception as e:
        print("Error : ",e)