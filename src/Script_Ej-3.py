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

#Ej 3-
print("Ejercicio 3")

for i in df.columns:
    df = df.withColumnRenamed(i, i[:-3])

df = df.withColumnRenamed("Fe", "Fecha")

df.withColumnRenamed("Fecha", "Día").show(10)



df_limpio = df.drop("Fecha")
df_limpio_2 = df.drop("Fecha")

#for i in df_limpio.columns:
    #df_limpio.agg({i: "min"}).show(truncate=False)
    #df_limpio.agg({i: "max"}).show(truncate=False)
    #df_limpio.agg({i: "avg"}).show(truncate=False) #Hacer join después.
    #Lo he hecho sin hacer cast porque no me he dado cuenta.


df_full = df_limpio_2.withColumn("Definciecy Notice UNI", when(col("UNI") < 1, TRUE).otherwise(FALSE))
df_full.printSchema()
df_full.show(100)
