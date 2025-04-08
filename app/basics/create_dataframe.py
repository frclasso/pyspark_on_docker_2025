from app.spark_connection import spark_conn

# Sample data: list of tuples
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Define column names
columns = ["Name", "Age"]

# Create a DataFrame from the data
df = spark_conn.createDataFrame(data, columns)

# Show the DataFrame content
df.show()
print()

print('Select the "Name" column')
df.select("Name").show()
print()

print("Select multiple columns")
df.select("Name", "Age").show()
print()

("Filter rows where age is greater than 30")
df.filter(df.Age > 30).show()
print()

print("Group by age and count occurrences")
df.groupBy("Age").count().show()
print()

print("Sort the DataFrame by age")
df.orderBy("Age").show()
print()

print("Sort by age in descending order")
df.orderBy(df.Age.desc()).show()
print()

# Stop the SparkSession
spark.stop()