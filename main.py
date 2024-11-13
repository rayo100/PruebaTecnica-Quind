from pyspark.sql import SparkSession
from src.logger import logger
from src.etl import ETLProcess

def main():
    """
    Función principal que gestiona el flujo de trabajo ETL.
    """

    # Definir la ruta al archivo Excel
    file_path = r"data\Films_2.xlsx"

    # Crear una instancia de ETLProcess
    etl = ETLProcess(logger)

    # Extraer los datos
    data = etl.extract_data(file_path)

    if data:
        logger.info("Datos extraídos exitosamente.")

        # Transformar los datos
        transformed_data = etl.transform_data(data)

        if transformed_data:
            logger.info("Datos transformados exitosamente.")

            # Cargar los datos
            etl.load_data(transformed_data)

if __name__ == "__main__":
    main()
