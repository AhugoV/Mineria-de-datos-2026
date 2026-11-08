import yfinance as yf

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


from Spark_session import spark_session
from Database_spark import database_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = spark_session()

print("Ej 2_a")

# Al no usar inferSchema, Spark carga todo como texto plano por defecto.
df_sentimiento = spark.read.option("header", True).option("sep", ";").csv("aplicacion2026\data\sentimiento_ibex.csv")

# Observamos los tipos de datos
df_sentimiento.printSchema()

print(
    "Un archivo CSV es simplemente un formato de texto plano y no guarda datos internos como el tipo de dato de cada columna."
)

print("Ej 2_b")

# Guardamos en la capa bronze en formato Parquet
# Usamos overwrite para la carga inicial como buena práctica
df_sentimiento.write.mode("overwrite").parquet("aplicacion2026/data/lake/bronze/sentimiento")

# Número de filas del DataFrame
filas_escritas = df_sentimiento.count()
print(f"Filas escritas en la capa bronze: {filas_escritas}")