from app.utils.spark_connection import spark_conn

# Sample data: list of tuples
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Define column names
columns = ["Name", "Age"]

# Create a DataFrame from the data
people_df = spark_conn.createDataFrame(data, columns)

# Show the DataFrame content
people_df.show()
print()

print('Select the "Name" column')
people_df.select("Name").show()
print()

print("Select multiple columns")
people_df.select("Name", "Age").show()
print()

("Filter rows where age is greater than 30")
people_df.filter(df.Age > 30).show()
print()

print("Group by age and count occurrences")
people_df.groupBy("Age").count().show()
print()

print("Sort the DataFrame by age")
people_df.orderBy("Age").show()
print()

print("Sort by age in descending order")
people_df.orderBy(df.Age.desc()).show()
print()

# Stop the SparkSession
# spark_conn.stop()