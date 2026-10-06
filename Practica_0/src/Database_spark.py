import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def database_spark(data_frame):

	propiedades = {
		"user": "root",
		"password": "LuaSimba5",
		"driver": "com.mysql.cj.jdbc.Driver"
	}

	data_frame.write.jdbc(
		url="jdbc:mysql://localhost:3306/ibex35", 
		table="ibex35_data", 
		mode="overwrite", 
		properties= propiedades
		)

