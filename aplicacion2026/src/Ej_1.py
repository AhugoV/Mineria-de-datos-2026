import yfinance as yf

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


from Spark_session import spark_session
from Database_spark import database_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = spark_session()

print("Ej 1_a")

def descargar_histórico(ticker, inicio, fin): 

    pdf = yf.Ticker(ticker).history(start=inicio, end=fin, interval="1d", auto_adjust=False)

    pdf = pdf.reset_index()
    df_pyspark = spark.createDataFrame(pdf)
    df_pyspark.show()

    return df_pyspark

df = descargar_histórico("BBVA", "2024-10-3", "2026-10-3")
df.show()

print("Ej 1_b")

df = spark.read.option("header", True).option("sep", ";").option("dateFormat",
"yyyy/MM/dd").csv(r"aplicacion2026\data\ibex35_close-2024.csv")

df = df.drop("Fecha")

lista_columnas = df.columns

df_ibex_completo = None

for i in lista_columnas: 

    df = descargar_histórico(i, "2024-10-1", "2026-10-1")

    if df_ibex_completo is None:
        df_ibex_completo = df

    else:    
        df_ibex_completo = df_ibex_completo.union(df)

df.write.mode("overwrite").parquet("aplicacion2026\data/lake/bronze/ibex")
print(f"Total registros guardados: {df_ibex_completo.count()}")





