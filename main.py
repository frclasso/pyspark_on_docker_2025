from datetime import datetime
from utils.spark_connection import spark_conn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



import sys
sys.path.append('/opt/bitnami/spark/app')

# Read table data raw_people
try:
    # Read table data raw_people
    raw_df = spark_conn.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/sparkdb") \
        .option("dbtable", "raw_people") \
        .option("user", "sparkuser") \
        .option("password", "sparkpass") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    logging.info("DataFrame created successfully.")
    raw_df.show(20, truncate=False)

except Exception as e:
    logging.error(f"Error reading from database: {str(e)}")
    raise