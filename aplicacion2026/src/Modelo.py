import yfinance as yf
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from delta.tables import DeltaTable
from Spark_session import spark_session
from Database_spark import database_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def ejecutar_ej3(spark):
    # Usar las comillas invertidas dentro del string rodeando la ruta.
    DeltaTable.convertToDelta(spark, "parquet.`aplicacion2026/data/lake/bronze/ibex`")
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

def descargar_historico(spark, ticker, inicio, fin):

    pdf = yf.Ticker(ticker).history(start=inicio, end=fin, interval="1d", auto_adjust=False)
    pdf = pdf.reset_index()
    
    df_pyspark = spark.createDataFrame(pdf)
    return df_pyspark

def ejecutar_ej1b(spark, df_tickers, inicio, fin):

    lista_columnas = df_tickers.columns
    df_ibex_completo = None

    for ticker in lista_columnas:
        # Añadimos .MC si no lo tiene, ya que Yahoo Finance lo requiere para el Mercado Continuo español
        ticker_yf = ticker if ticker.endswith(".MC") else f"{ticker}.MC"
        
        df = descargar_historico(spark, ticker_yf, inicio, fin)
        if df_ibex_completo is None:
            df_ibex_completo = df
        else:    
            df_ibex_completo = df_ibex_completo.union(df)

    df.write.mode("overwrite").parquet("aplicacion2026\data/lake/bronze/ibex")
    return df_ibex_completo

def ejecutar_ej2a(spark, ruta_csv):

    df_sentimiento = spark.read.option("header", True).option("sep", ";").csv(ruta_csv)
    df_sentimiento.printSchema()
    return df_sentimiento

def ejecutar_ej2b(df_sentimiento):

    df_sentimiento.write.mode("overwrite").parquet("aplicacion2026/data/lake/bronze/sentimiento")

    # Número de filas del DataFrame
    filas_escritas = df_sentimiento.count()
    print(f"Filas escritas en la capa bronze: {filas_escritas}")