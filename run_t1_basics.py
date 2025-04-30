import os
from time import time, sleep
from datetime import datetime
from utils.spark_connection import spark_conn
import logging
from pyspark.sql.functions import *

from app.t1_basics.gen_fake_records import  records_df
from app.t1_basics.gen_churn_records import churn1_records_df
from app.t1_basics.churn_1 import cleaning_data, churn_feature_engineering,churn_model, evaluate_model


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
 