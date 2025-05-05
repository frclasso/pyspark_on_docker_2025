from pyspark.sql.window import Window
from pyspark.sql.functions import *
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





