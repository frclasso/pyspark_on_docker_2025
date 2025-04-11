from datetime import datetime
from utils.spark_connection import spark_conn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# import sys
# sys.path.append('/opt/bitnami/spark/app')

# GENERATE AND INGEST RAW DATA
from app.incremental_processing.gen_raw_data import generate_raw_data, persisting_data

raw_df = generate_raw_data(spark_conn)
if raw_df is not None:
    raw_schema = raw_df.printSchema()
    raw_df.show(truncate=False)
    persisting_data(raw_df)


# # Read table data raw_people
# try:
#     # Read table data raw_people
#     raw_df = spark_conn.read.format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres:5432/sparkdb") \
#         .option("dbtable", "raw_people") \
#         .option("user", "sparkuser") \
#         .option("password", "sparkpass") \
#         .option("driver", "org.postgresql.Driver") \
#         .load()

#     logging.info("DataFrame created successfully.")
#     print("JDBC Driver:", spark_conn.sparkContext.getConf().get("spark.jars"))
#     print("JDBC URL:", "jdbc:postgresql://postgres:5432/sparkdb")
#     raw_df.show(20, truncate=False)

# except Exception as e:
#     logging.error(f"Error reading from database: {str(e)}")
#     raise