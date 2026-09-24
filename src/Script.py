import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_session import spark_session
from Database_spark import database_spark

spark = spark_session()

data_frame = spark.read.option("header", True).option("sep", ";").option("dateFormat",
"dd/MM/yyyy").csv(r"data\ibex35_close-2024.csv")

data_frame.show()

database_spark(data_frame)

