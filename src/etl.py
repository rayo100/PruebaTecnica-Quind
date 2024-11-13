from pyspark.sql import SparkSession
import pandas as pd


class ETLProcess:
    def __init__(self, logger):
        """
        Inicializa el proceso ETL, creando una sesión de Spark y configurando el logger.
        """
        self.logger = logger
        self.spark = SparkSession.builder.appName("ETLFilms").getOrCreate()

    def extract_data(self, file_path):
        """
        Extrae los datos de un archivo Excel.

        :param file_path: Ruta del archivo Excel.
        :return: Un diccionario con los DataFrames de cada hoja.
        """
        try:
            self.logger.info("Extrayendo datos del archivo Excel.")
            data = pd.read_excel(file_path, sheet_name=None)
            self.logger.info("Datos extraídos con éxito.")
            return data
        except Exception as e:
            self.logger.error(f"Error al extraer datos: {e}")
            return None

    def transform_data(self, data):
        """
        Realiza las transformaciones necesarias a los datos extraídos.

        :param data: Diccionario de DataFrames extraídos.
        :return: DataFrame transformado listo para cargar.
        """
        try:
            self.logger.info("Iniciando transformación de los datos.")

            # Transformar los datos, ejemplo de cambio de columnas, agregación, etc.
            # (Implementa tus transformaciones según los requisitos del negocio)

            # Ejemplo de transformación usando Spark: convertir DataFrame de Pandas a Spark
            df_spark = self.spark.createDataFrame(data['Movies'])
            self.logger.info("Transformación completada.")
            return df_spark
        except Exception as e:
            self.logger.error(f"Error al transformar los datos: {e}")
            return None

    def load_data(self, df_spark):
        """
        Carga los datos transformados a un destino (base de datos, archivo, etc.).

        :param df_spark: DataFrame transformado de Spark.
        :return: Confirmación de la carga.
        """
        try:
            self.logger.info("Cargando datos en el destino.")
            # Implementar carga en una base de datos, archivo, etc.
            # Por ejemplo, guardar como archivo CSV o cargar a base de datos:
            df_spark.write.csv('data/output.csv', header=True)
            self.logger.info("Datos cargados con éxito.")
        except Exception as e:
            self.logger.error(f"Error al cargar los datos: {e}")

