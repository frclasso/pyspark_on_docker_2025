from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import sys
sys.path.append('/opt/bitnami/spark/app')

def read_table_raw_people(spark_conn):
    """Read table data raw_people"""
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
        return raw_df
    except Exception as e:
        logging.error(f"Error reading from database: {str(e)}")
        return None
