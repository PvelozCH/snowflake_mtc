import logging
import sys

def setup_logger():
    """
    Configura un logger centralizado para escribir en un archivo y en la consola.
    """
    # Obtener el logger raíz.
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) 

    # Evita que se agreguen múltiples handlers si se llama varias veces.
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formato del log
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler para escribir en el archivo Log.txt
    file_handler = logging.FileHandler("Log.txt", mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Handler para mostrar en la consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Agregar los handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Inicializar y exportar el logger para ser usado en otros módulos.
logger = setup_logger()

def start_run_log(parametro):
    """
    Añade un encabezado al log para identificar el inicio de una nueva ejecución.
    """
    logger.info("=====================================================================")
    logger.info(f"INICIANDO NUEVA EJECUCIÓN CON PARÁMETRO: {parametro.upper()}")
    logger.info("=====================================================================")