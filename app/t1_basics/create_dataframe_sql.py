from app.utils.spark_connection import spark

# Sample data: list of tuples
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Define column names
columns = ["Name", "Age"]

# Create a DataFrame from the data
people_df = spark.createDataFrame(data, columns)

# Register the DataFrame as a temporary view
people_df.createOrReplaceTempView("people")

# Run a SQL query
result = spark.sql("SELECT Name, Age FROM people WHERE Age > 30")

# Show the result of the query
result.show()


print("Select only the 'Name' column")
result = spark.sql("SELECT Name FROM people")
# result.show()
# print()

print("Select  'Age  < 30' ")
result = spark.sql("SELECT * FROM people WHERE Age < 30")
# result.show()
# print()

print("Count people by age")
result = spark.sql("SELECT Age, COUNT(*) as count FROM people GROUP BY Age")
# result.show()
