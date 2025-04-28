from pyspark import SparkContext, SparkConf

# Configure Spark
conf = SparkConf().setMaster("local[3]").setAppName("RDD example")
sc = SparkContext.getOrCreate(conf=conf)

# Create RDD
numbers_rdd = sc.parallelize(range(1, 100))
# Count the number of elements in the RDD
print(f"Numbers_rdd output:{numbers_rdd.count()}")
print()

# Collect data
collected_number = numbers_rdd.collect()

# Print the output
print(f"Collected_numbers output:{collected_number}")
print(collected_number)
print()

# Multiply each element by 2
doubled_rdd = numbers_rdd.map(lambda x: x * 2)

# Collect and print the result
print(f"Doubled_rdd output:{doubled_rdd.collect()}")


# Stop the SparkContext
sc.stop()
