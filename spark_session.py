import os

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.2" 

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()


data_frame = spark.read.option("header", True).option("sep", ";").option("dateFormat",
"dd/MM/yyyy").csv("ibex35_close-2024.csv")

data_frame.show()