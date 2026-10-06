import os

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.2" 

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def spark_session():

    spark = SparkSession.builder \
        .appName("IBEX35") \
        .config('spark.driver.extraClassPath', r".\Connection\mysql-connector-j-9.5.0.jar") \
        .getOrCreate()\

    return spark

