from pyspark.sql.window import Window
from pyspark.sql.functions import *
from app.spark_connection import spark_conn
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


"""
Window functions allow you to perform calculations across a set of rows that are related to the current row. 
These are particularly useful for tasks like ranking, moving averages, and cumulative sums.
"""

# Schemas
people_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

jobs_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Job", StringType(), True)
])

# Sample data: list of tuples
people_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Fabio", 50), ('Marx', 107)]
data_jobs = [("Alice", "Engineer"), ("Bob", "Doctor"), ("Charlie", 'Teacher'), ("Fabio", 'Data Engineer'), ('Marx', None)]


# Define column names
columns = ["Name", "Age"]

# Create a DataFrame from the data
df_people = spark_conn.createDataFrame(people_data, columns)

# Dataframe 2
df_jobs = spark_conn.createDataFrame(data_jobs, ["Name", "Job"])

# Register the DataFrame as a temporary view
df_people.createOrReplaceTempView("people")
df_jobs.createOrReplaceTempView("jobs")
result_df = df_people.join(df_jobs, "Name", "inner")

# Define windows spaecification
window_spec = Window.orderBy("Age") # asc

# Add rank column
df_with_rank = result_df.withColumn("rank", rank().over(window_spec))
# df_with_rank.show()

# Cache the DataFrame
df_with_rank.cache()

# Perform operations on the cached DataFrame
df_with_rank.show()