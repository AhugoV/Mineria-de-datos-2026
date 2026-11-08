import os
import sys
import pandas as pd
import yfinance as yf
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuración del worker de Python en Windows si fuera necesario[cite: 33]

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.2" 
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Inicialización de SparkSession configurada para Delta Lake[cite: 34, 35]
builder = (
    SparkSession.builder.appName("IBEX35")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ==========================================
# Ej1a
# ==========================================
print("Ej1a")


def descargar_historico(ticker: str, start: str, end: str):
    """Descarga el histórico de un ticker con yfinance entre dos fechas y lo devuelve en un DataFrame de PySpark."""
    # Descarga de datos con yfinance sin ajuste automático
    pdf = yf.Ticker(ticker).history(
        start=start, end=end, interval="1d", auto_adjust=False)

    if pdf.empty:
        return None

    # yfinance usa la fecha como índice; reset_index la convierte en columna ordinaria
    pdf.reset_index(inplace=True)

    # Añadir columna con el símbolo bursátil
    pdf["Ticker"] = ticker

    # Normalizar la fecha a tipo fecha (remover zona horaria de pandas si la hubiera)
    if "Date" in pdf.columns:
        pdf["Date"] = pd.to_datetime(pdf["Date"]).dt.date

    # Conversión del DataFrame de pandas a Spark DataFrame[cite: 31]
    df_spark = spark.createDataFrame(pdf)
    return df_spark


# Definición de nulidad por columna (justificación técnica según el documento):
# - Date / Fecha: NO NULO. Es la clave temporal de la observación; sin ella el registro pierde significado cronológico.
# - Open, High, Low, Close: NO NULOS. Representan la cotización real de la sesión; si la bolsa operó, existen precios.
# - Volume: NULO PERMITIDO. Refleja el volumen negociado; en festivos parciales o fallos de reporte puede registrarse vacío[cite: 30].
# - Ticker: NO NULO. Es el identificador del activo financiero imprescindible para diferenciar empresas[cite: 30].
# - Dividends: NO NULO. Toma por defecto el valor 0.0 en días sin reparto de dividendos[cite: 30].
# - Stock Splits: NO NULO. Toma por defecto el valor 0.0 cuando no hay desdoblamiento de títulos[cite: 31].

# Prueba de la función con un valor de ejemplo[cite: 30]
df_test = descargar_historico("BBVA.MC", "2024-10-01", "2026-10-01")
df_test.printSchema()
df_test.show(5)

# ==========================================
# Ej1b
# ==========================================
print("Ej1b")

# Lista oficial de empresas que componen el IBEX-35 (con sufijo .MC)
tickers_ibex = [
    "ANA.MC",
    "ANE.MC",
    "ACX.MC",
    "ACS.MC",
    "AENA.MC",
    "AMS.MC",
    "MTS.MC",
    "BKT.MC",
    "BBVA.MC",
    "CABK.MC",
    "CLNX.MC",
    "COL.MC",
    "ELE.MC",
    "ENG.MC",
    "FDR.MC",
    "FER.MC",
    "GRF.MC",
    "IAG.MC",
    "IBE.MC",
    "IDR.MC",
    "ITX.MC",
    "LOG.MC",
    "MAP.MC",
    "MEL.MC",
    "MRL.MC",
    "NTGY.MC",
    "PUIG.MC",
    "RED.MC",
    "REP.MC",
    "ROVI.MC",
    "SAB.MC",
    "SAN.MC",
    "SCYR.MC",
    "SLR.MC",
    "TEF.MC",
    "UNI.MC",
]

# Rango de 2 años finalizando el 1 de octubre de 2026[cite: 33]
start_date = "2024-10-01"
end_date = "2026-10-01"

df_ibex_completo = None

for ticker in tickers_ibex:
    df_ticker = descargar_historico(ticker, start_date, end_date)
    if df_ticker is not None:
        if df_ibex_completo is None:
            df_ibex_completo = df_ticker
        else:
            # Apilado sucesivo de filas por posición[cite: 32]
            df_ibex_completo = df_ibex_completo.union(df_ticker)

# Guardar crudo en formato Parquet en la capa bronze[cite: 32, 33]
df_ibex_completo.write.mode("overwrite").parquet("aplicacion2026/data/lake/bronze/ibex")
print(f"Total registros guardados en Parquet: {df_ibex_completo.count()}")

# ==========================================
# Ej1c (opcional)
# ==========================================
print("Ej1c")

# 1. Leer de vuelta el Parquet recién escrito y mostrar esquema y 5 filas[cite: 32, 33]
df_bronze = spark.read.parquet("aplicacion2026/data/lake/bronze/ibex")
df_bronze.printSchema()
df_bronze.show(5)

# 2. Aplicar column pruning leyendo únicamente Ticker, Date y Close[cite: 32, 33]
df_pruned = spark.read.parquet("aplicacion2026/data/lake/bronze/ibex").select(
    "Ticker", "Date", "Close"
)
df_pruned.show(5)

# Justificación técnica solicitada[cite: 31, 32]:
print(
    "Ventaja del column pruning: Al ser Parquet un formato columnar, los datos de cada columna se guardan contiguos en 'column chunks' independientes[cite: 31, 32]. "
    "Al seleccionar únicamente Ticker, Date y Close, Spark lee exclusivamente los bloques de esas columnas y descarta por completo los de Open, High, Volume, etc., "
    "reduciendo drásticamente las operaciones de E/S (disco y red) y el uso de memoria RAM[cite: 31, 32]."
)