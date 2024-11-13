import logging


def setup_logger():
    """
    Configura un logger para la aplicación.

    Crea un logger que muestra los mensajes en la consola.

    :return: El logger configurado.
    """
    # Crear un logger llamado "ETLLogger"
    logger = logging.getLogger("ETLLogger")

    # Establecer el nivel de log para capturar mensajes desde DEBUG hacia arriba
    logger.setLevel(logging.DEBUG)

    # Crear un manejador que enviará los logs a la consola
    handler = logging.StreamHandler()

    # Establecer el nivel de los logs que el manejador capturará
    handler.setLevel(logging.DEBUG)

    # Definir el formato de los mensajes de log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Asignar el formato al manejador
    handler.setFormatter(formatter)

    # Añadir el manejador al logger
    logger.addHandler(handler)

    # Retornar el logger
    return logger


# Configurar el logger
logger = setup_logger()

# Ejemplo de uso: registrar diferentes tipos de mensajes
logger.debug("Este es un mensaje de depuración.")
logger.info("Este es un mensaje informativo.")
logger.warning("Este es un mensaje de advertencia.")
logger.error("Este es un mensaje de error.")
logger.critical("Este es un mensaje crítico.")
