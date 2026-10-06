import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_session import spark_session
from Database_spark import database_spark

spark = spark_session()

df = spark.read.option("header", True).option("sep", ";").option("dateFormat",
"yyyy/MM/dd").csv(r"data\ibex35_close-2024.csv")

#Ej2-a
print("Ejercicio 2-a")

n = df.count()
print(n, "filas")

df.show(truncate=False)
df_new = df.distinct()

n = df_new.count()
print(n, "filas")

#Ej 2-b
print("Ejercicio 2-b")

print("Fecha mínima y máxima de los datos del DataFrame")
df_new.agg({"Fecha": "min"}).show(truncate=False)
df_new.agg({"Fecha": "max"}).show(truncate=False)

print("Hay ", n-1, " días de información de las empresas")

