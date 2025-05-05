from app.utils.spark_connection import spark_conn
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import coalesce, lit, col


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
people_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Fabio", None), ('Marx', None)]
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

# SQL join query
# result_df = spark_conn.sql("""
#     SELECT p.Name, p.Age, j.Job 
#     FROM people p
#     JOIN jobs j ON p.Name = j.Name
# """)
result_df = df_people.join(df_jobs, "Name", "inner")

print("Join dataframes")
result_df.show()

print("Drop rows with any missing values ===================================================")
df_cleaned = result_df.dropna()
df_cleaned.show()

print("Drop rows where the Age column has missing values ===================================================")
# df_cleaned_age = result_df.dropna(subset=["Age"])
df_cleaned_age = result_df.filter(col("Age").isNotNull())
# df_cleaned_age = result_df.dropna(subset=coalesce(result_df["Age"], lit(0)) < 30)
# df_cleaned_age.show()


print("fill missing valuess ===================================================")
# df_filled = result_df.fillna({"Age": 50, "Name": "Fabio"})
df_filled = result_df.na.fill({"Age": 50}) # fillna is an alias for na.fill
# df_filled.show() 