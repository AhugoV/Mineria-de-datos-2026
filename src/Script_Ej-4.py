import os
from tkinter import FALSE, TRUE

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_session import spark_session
from Database_spark import database_spark

spark = spark_session()

df = spark.read.option("header", True).option("sep", ";").option("dateFormat",
"yyyy/MM/dd").csv(r"data\ibex35_close-2024.csv")

df = df.drop("Fecha")

for i in df.columns:
    df = df.withColumnRenamed(i, i[:-3])

df_new = df

for i in df_new.columns:

    df_new = df_new.withColumn(i, df_new[i].cast("double"))

for i in range(df_new.count()):

    primera_fila = df_new.head(1)[0]
    ultima_fila = df_new.tail(1)[0]

    valor_inicial = primera_fila[i]
    valor_final = ultima_fila[i]
    
    diferencia = valor_inicial - valor_final
    V_anual = diferencia / valor_inicial * 100

    df_new = df.withColumn("Clasificacion", when(15 <= V_anual, "Subida fuerte") 
        .otherwise(when(2 < V_anual < 15, "Subida")) 
        .otherwise(when(V_anual <= -15, "Bajada fuerte") ) 
        .otherwise(when(0 > V_anual > -15, "Bajada")) 
        .otherwise("Neutra"))

df_new.show(100)
