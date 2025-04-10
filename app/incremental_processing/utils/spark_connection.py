from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark_conn = (SparkSession.builder
               .appName("IncrementalDataProcessing")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .config("spark.kryoserializer.buffer.max", "512m")
               .config("spark.eventLog.enabled", "true")
               .getOrCreate()
            )