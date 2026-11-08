import yfinance as yf

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import datetime

from delta.tables import DeltaTable
from Spark_session import spark_session
from Database_spark import database_spark
from Ej_1 import descargar_histórico
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = spark_session()

print("Ej4")

df = spark.read.option("header", True).option("sep", ";").option("dateFormat",
"yyyy/MM/dd").csv(r"aplicacion2026\data\ibex35_close-2024.csv")

# 1. Visualizar la versión actual de la base de datos[cite: 37, 38]
destino_ibex = DeltaTable.forPath(spark, "aplicacion2026/data/lake/bronze/ibex")

# Extraer el número de la versión más reciente consultando el log[cite: 37]
version_actual = destino_ibex.history(1).select("version").collect()[0][0]
print(f"Versión actual de la tabla Delta 'ibex' antes del MERGE: {version_actual}")

# Contar filas antes de añadir los datos nuevos para poder comparar
filas_antes = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex").count()
print(f"Filas actuales en la tabla: {filas_antes}")

# 2. Descargar los precios del IBEX-35 hasta "hoy"
# Para que yfinance descargue hasta el día actual incluido, le pasamos 'mañana' como fin.
hoy_mas_uno = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
start_date_nuevos = "2026-10-01" 

df = df.drop("Fecha")

lista_columnas = df.columns

df_nuevos_datos = None
for ticker in lista_columnas:
    # Usamos la función creada en el Ej1a
    df_ticker = descargar_histórico(ticker, start_date_nuevos, hoy_mas_uno)
    if df_nuevos_datos is None:
            df_nuevos_datos = df_ticker
    else:
            df_nuevos_datos = df_nuevos_datos.union(df_ticker)

# En pandas/yfinance la columna por defecto se llama "Date". Si en tu código 
# anterior la renombraste, cambia "Date" por "Fecha" en la condición de abajo.
col_fecha = "Date" if "Date" in df_nuevos_datos.columns else "Fecha"

# Primera ejecución del MERGE
print("\nEjecutando la primera operación MERGE...")
(destino_ibex.alias("d")
 .merge(df_nuevos_datos.alias("n"), f"d.Ticker = n.Ticker AND d.{col_fecha} = n.{col_fecha}")
 .whenMatchedUpdateAll()     # Actualiza valores si el día ya existía pero hubo correcciones (ej. cierre de sesión)
 .whenNotMatchedInsertAll()  # Inserta los días totalmente nuevos
 .execute())

filas_despues_1 = spark.read.format("delta").load("data/lake/bronze/ibex").count()
print(f"Filas tras la 1ª ejecución: {filas_despues_1}")

# Segunda ejecución del MERGE]
print("\nEjecutando la segunda operación MERGE (idéntica a la primera)...")
(destino_ibex.alias("d")
 .merge(df_nuevos_datos.alias("n"), f"d.Ticker = n.Ticker AND d.{col_fecha} = n.{col_fecha}")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

filas_despues_2 = spark.read.format("delta").load("data/lake/bronze/ibex").count()
print(f"Filas tras la 2ª ejecución: {filas_despues_2}")

# Comentario
print(
    "\nEn la primera ejecución, el número de filas ha aumentado porque se "
    "han insertado los registros de los días nuevos que acabamos de descargar. "
    "Al ejecutarlo por segunda vez, el número de filas se mantiene exactamente igual. "
    "Esto sucede porque Delta Lake soporta operaciones 'idempotentes' mediante el MERGE: "
    "al encontrar que la combinación (Ticker, Fecha) ya existe en la tabla, sabe que no "
    "debe insertarla como una fila nueva, evitando así que los datos se dupliquen."
)