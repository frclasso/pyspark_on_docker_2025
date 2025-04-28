from time import time, sleep
from datetime import datetime
from utils.spark_connection import spark_conn
import logging
from pyspark.sql.functions import *
from app.window_functions.customer_by_category import ApplyRanking, ApplyLagLead
from app.window_functions.gen_fake_records import records_df

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Show sample data
# print(records_df.head())
# print()

customer_df = spark_conn.createDataFrame(records_df)
logging.info("Customer by category dataframe")
customer_df.show(10)
print()

logging.info("Customer by category dataframe Ranking")
df_ranked = ApplyRanking(customer_df)
df_ranked.show(10)
print()

logging.info("Customer by category dataframe Trends")
df_trends = ApplyLagLead(customer_df)
df_trends.show(100)    