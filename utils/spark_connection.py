from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark_conn = (SparkSession.builder
               .appName("IncrementalDataProcessing")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .config("spark.kryoserializer.buffer.max", "512m")
               .config("spark.eventLog.enabled", "true")
               .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar")
               .getOrCreate()
            )



# spark_conn = SparkSession.builder \
#     .appName("PostgreSQL Connection") \
#     .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
#     .getOrCreate()
