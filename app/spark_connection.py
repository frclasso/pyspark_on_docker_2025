from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark_conn = SparkSession.builder \
   .appName("My App") \
   .getOrCreate()