import os
from time import time, sleep
from datetime import datetime
from utils.spark_connection import spark_conn
import logging
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from app.t1_basics.gen_fake_records import  records_df


# 1 Churn example
from app.t1_basics.gen_churn_records import churn1_records_df
from app.t1_basics.churn_1 import cleaning_data, churn_feature_engineering,churn_model, evaluate_model
print("Churn example ================================================================================")

#chrun records, pandas dataframe
churn_1_df = spark_conn.createDataFrame(data=churn1_records_df)
churn_1_df.printSchema()
churn_1_df.show(50, truncate=False)
print()

# churn indexed
churn_cleaned = cleaning_data(churn_1_df)
churn_cleaned.show(50, truncate=False)
print()

# churn feature engineering
chuun_data_prepared = churn_feature_engineering(churn_cleaned)
chuun_data_prepared.show(50, truncate=False)
print() 

# churn model predition
churn_model_pred = churn_model(data=churn_1_df)
churn_model_pred.show(50, truncate=False)
print()


# Evaluate the model
churn_roc_auc = evaluate_model(churn_model_pred)
print(f"Area under ROC curve: {churn_roc_auc}")


# 2 Ranking example
from app.t1_basics.ranking import people_data, data_jobs
print("Ranking example =================================================================================")
# Define column names
columns = ["Name", "Age"]

# Create a DataFrame from the data
df_people = spark_conn.createDataFrame(people_data, columns)
df_people.show()
print()

# Dataframe 2
df_jobs = spark_conn.createDataFrame(data_jobs, ["Name", "Job"])
df_jobs.show()
print()

# Register the DataFrame as a temporary view
vw_df_people = df_people.createOrReplaceTempView("people")
vw_df_jobs = df_jobs.createOrReplaceTempView("jobs")

result_df = df_people.join(df_jobs, "Name", "inner")

# Define windows spaecification
window_spec = Window.orderBy("Age") # asc

# Add rank column
df_with_rank = result_df.withColumn("rank", rank().over(window_spec))
# df_with_rank.show()

# Cache the DataFrame
df_with_rank.cache()

# Perform operations on the cached DataFrame
df_with_rank.show(truncate=False)
print()

# Vector Assembler example
print("Vector Assembler example =================================================================================") 
from app.t1_basics.vector_assembler_pipeline import vector_data,vector_schema, predictModel

df = spark_conn.createDataFrame(vector_data, schema=vector_schema)
vector_assembler_predictions = predictModel(dataframe=df, spark=spark_conn)
print("Vector Assembler predictions")
if vector_assembler_predictions:
    vector_assembler_predictions.show(truncate=False)
else:
    print("No predictions to show.")
print()