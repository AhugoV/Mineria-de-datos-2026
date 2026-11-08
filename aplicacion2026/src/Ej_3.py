import yfinance as yf

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


from Spark_session import spark_session
from Database_spark import database_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = spark_session()

print("Ej3")

# Usar las comillas invertidas dentro del string rodeando la ruta.
DeltaTable.convertToDelta(spark, "parquet.`data/lake/bronze/ibex`")
DeltaTable.convertToDelta(spark, "parquet.`aplicacion2026/data/lake/bronze/sentimiento`")

print("Historial de la tabla Delta 'ibex':")
DeltaTable.forPath(spark, "data/lake/bronze/ibex").history().show()

print("Historial de la tabla Delta 'sentimiento':")
DeltaTable.forPath(spark, "data/lake/bronze/sentimiento").history().show()

# _delta_log
print(
    "Comentario: Al convertir los ficheros, ha aparecido una nueva subcarpeta oculta llamada '_delta_log' dentro de cada directorio. "
    "Esta carpeta contiene el registro de transacciones compuesto por ficheros JSON. Esto es lo que convierte a Parquet "
    "en Delta, permitiendo tener transacciones ACID, actualizar, borrar y recuperar versiones anteriores sin mover ni reescribir los datos originales."
)