# -*- coding: utf-8 -*-
"""
Punto de entrada principal para el proceso de extracción de datos de Snowflake.

Este script se conecta a Snowflake, ejecuta consultas para obtener órdenes de trabajo (OT) y sus comentarios,
y procesa estos datos para almacenarlos en una base de datos SQLite local.
Finalmente, puede generar archivos JSON a partir de los datos locales.

El comportamiento del script se controla mediante un parámetro de línea de comandos:
- 'historico': Realiza una carga completa. Procesa todas las OT y comentarios desde Snowflake,
  los guarda en SQLite y genera un archivo JSON con todos los datos históricos.
- 'temp': Realiza una carga incremental. Compara los datos de Snowflake con los existentes en SQLite
  y procesa solo los comentarios nuevos, generando un JSON temporal para ellos.
- 'jsonhistorico': Omite la extracción de Snowflake y genera el JSON histórico a partir de los
  datos que ya se encuentran en la base de datos SQLite.
"""

import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.snowpark import Session
import json, sys
from datetime import datetime, date
import sqlite3
import requests

from snowflake_servicios import crear_ot, crear_comentarios, log, agregarEnLog
from carga_servicios import jsonOt, jsonHistorico, jsonTemp

try:
    # --- 1. CONFIGURACIÓN Y CONEXIONES ---
    
    # Define los parámetros de conexión para Snowflake.
    # Se recomienda externalizar estas credenciales en un futuro (p. ej., usando variables de entorno con .env).
    conn2 = {
        "WAREHOUSE": "DEFAULT_WAREHOUSE",
        "ACCOUNT": "BHP-SYDNEY",
        "USER": "NICOLAS.JIMENEZ1@BHP.COM",
        "AUTHENTICATOR": "externalbrowser",
        "ROLE": "SNOWFLAKE_READER_PROD",
        "DATABASE": "GLOBAL_PROD",
        "SCHEMA": "QA_DU_STANDARDISED_WORK_MINAM"
    }
    # Crea la sesión de Snowpark
    session = Session.builder.configs(conn2).create()
    print("Se conectó a SnowFlake!")

    # Conecta a la base de datos local SQLite
    connSqlite = sqlite3.connect("BDD_SNOWFLAKE.db")
    print("Se conectó a sqlite!")

    # --- 2. DEFINICIÓN DE QUERIES ---

    # Query para obtener la lista única de órdenes de trabajo (OT).
    queryInicio = "SELECT DISTINCT activity_id, sap_work_number FROM sw_temp_maintainer_comments WHERE activity_mwc = 'SN16'"

    # Query completa para obtener los detalles de los comentarios, uniéndolos con información adicional.
    query = f'''
        WITH TodasLasOT AS (
            -- Primero, obtenemos una lista de todas las OT relevantes
            SELECT DISTINCT activity_id, sap_work_number
            FROM sw_temp_maintainer_comments
            WHERE activity_mwc = 'SN16'
        ),
        ComentariosPorOT AS (
            -- Luego, obtenemos los comentarios y los unimos con detalles del elemento
            SELECT 
                a.id, a.activity_id, a.sap_work_number, b.role_name, b.work_sequence_name,
                b.element_step, a.element_instance_name, a.suffix, a.comment_title,
                a.comment_description, a.location_urls, a.comment_used_for, a.created_date
            FROM sw_temp_maintainer_comments AS a
            LEFT JOIN sw_element_instance AS b ON a.element_instance_id = b.id
            WHERE a.comment_used_for IN ('Notification', 'Report')
        )
        -- Finalmente, filtramos los comentarios para asegurarnos de que pertenecen a las OT que nos interesan
        SELECT 
            c.id, t.activity_id, t.sap_work_number, c.role_name, c.work_sequence_name,
            c.element_step, c.element_instance_name, c.suffix, c.comment_title,
            c.comment_description, c.location_urls, c.comment_used_for, c.created_date
        FROM ComentariosPorOT AS c
        INNER JOIN TodasLasOT AS t ON c.activity_id = t.activity_id AND c.sap_work_number = t.sap_work_number
        ORDER BY t.sap_work_number, c.id;
    '''

except Exception as e:
    print(f"Error al conectar o ejecutar la consulta inicial: {e}")
    # Si hay un error en la conexión o preparación, se detiene la ejecución.
    sys.exit(1)


# --- 3. LÓGICA DE EJECUCIÓN PRINCIPAL ---

# Lee el primer argumento de la línea de comandos para determinar el modo de ejecución.
if len(sys.argv) < 2:
    print("Error: Debe proporcionar un parámetro de ejecución (historico, temp, jsonhistorico).")
    sys.exit(1)
    
parametro = sys.argv[1].lower()

# Inicializa el archivo de log para la ejecución actual
log()

if parametro == "historico":
    print("--- MODO HISTÓRICO ---")
    # Procesa la lista de OT y luego todos los comentarios desde Snowflake
    crear_ot(session, queryInicio, connSqlite)
    crear_comentarios(session, query, connSqlite, parametro) 
    # Genera el archivo JSON con todos los datos históricos de la BDD local
    jsonHistorico()
    connSqlite.commit()
    connSqlite.close()
    print("--- PROCESO HISTÓRICO COMPLETADO ---")

elif parametro == "temp":
    print("--- MODO TEMPORAL (INCREMENTAL) ---")
    # Procesa la lista de OT para tener una referencia actualizada
    crear_ot(session, queryInicio, connSqlite) 
    # Procesa solo los comentarios nuevos y genera un JSON temporal para ellos
    crear_comentarios(session, query, connSqlite, parametro)
    connSqlite.commit()
    connSqlite.close()
    print("--- PROCESO TEMPORAL COMPLETADO ---")
    
elif parametro == "jsonhistorico":
    print("--- MODO GENERAR JSON HISTÓRICO ---")
    # Genera el JSON histórico a partir de los datos ya existentes en SQLite
    jsonHistorico()
    connSqlite.commit()
    connSqlite.close()
    print("--- GENERACIÓN DE JSON HISTÓRICO COMPLETADA ---")

else:
    print(f"Error: Parámetro '{parametro}' no reconocido.")
    print("Los parámetros válidos son: historico, temp, jsonhistorico.")
