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

# Ej5
print("Ej5")

for i in df.columns:
    df = df.withColumnRenamed(i, i[:-3])

for i in df.columns:

    df = df.withColumn(i, df[i].cast("double"))

columnas_empresas = [c for c in df.columns if c not in ["Fe", "Dia"]]

for emp in columnas_empresas:
    
    q1, q2, q3 = df.approxQuantile(emp, [0.25, 0.5, 0.75], 0.01)
    
    nombre_col_cuartil = f"{emp} Cuartil"
    
    df = df.withColumn(
        nombre_col_cuartil,
        when(col(emp) <= q1, "q1")
        .when((col(emp) > q1) & (col(emp) <= q2), "q2")
        .when((col(emp) > q2) & (col(emp) <= q3), "q3")
        .otherwise("q4")
    )

df.show(1, truncate=False)

cols_aena_bbva = ["AENA", "AENA Cuartil", "BBVA", "BBVA Cuartil"]
df.select(*cols_aena_bbva).show(df.count(), truncate=False)