from pyspark.sql import SparkSession

# Create Spark session with specific configurations
spark = SparkSession.builder \
    .appName("StreamingExample") \
    .master("spark://spark-master:7077") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Your streaming code here
streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Write the output
query = streaming_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
