import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_session import spark_session
from Database_spark import database_spark

spark = spark_session()

#Ej 1-a

print("Ejercicio 1-a")

data_frame = spark.read.option("header", True).option("sep", ";").option("dateFormat",
"yyyy/MM/dd").csv(r"data\ibex35_close-2024.csv")

data_frame.printSchema()

data_frame.show(6)

data_frame_new = data_frame.withColumn("Fecha", regexp_replace(col("Fecha"), "/", "-"))
data_frame_new = data_frame_new.withColumn("Fecha", regexp_replace(col("Fecha"), r"(\d{2})-(\d{2})-(\d{4})", r"$3-$2-$1"))
data_frame_new = data_frame_new.withColumn("Fecha", col("Fecha").cast("Date"))

data_frame_new.printSchema()

data_frame_new.show(6)


#Ej 1-b

print("Ejercicio 1-b")

for i in data_frame_new.columns:
    data_frame_new = data_frame_new.withColumnRenamed(i, i[:-3])

data_frame_new = data_frame_new.withColumnRenamed("Fe", "Fecha")
data_frame_new.show(6)
data_frame_new.printSchema()

#Ej 2-a
print("Ejercicio 2-a")

a = data_frame_new.count()

print(a, "filas")

data_frame_new = data_frame_new.dropDuplicates()
data_frame_new.show(truncate=False)

b = data_frame_new.count()
print(b, "filas")
print("Se han eliminado", a-b, "filas duplicadas")
n = data_frame_new.columns
print("Hay información de:", len(n),"empresas")

#Ej 2-b
print("Ejercicio 2-b")

print("Fecha mínima y máxima de los datos del DataFrame")
data_frame_new.agg({"Fecha": "min"}).show(truncate=False)
data_frame_new.agg({"Fecha": "max"}).show(truncate=False)

print("Hay ", n-1, " días de información de las empresas")

#database_spark(data_frame_new)