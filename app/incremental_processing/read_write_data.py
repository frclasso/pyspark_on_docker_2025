from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import sys
sys.path.append('/opt/bitnami/spark/app')

def read_table(spark_conn, tableName):
    """Read table data raw_people"""
    try:
        # Read table data raw_people
        raw_df = spark_conn.read.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/sparkdb") \
            .option("dbtable", f"{tableName}") \
            .option("user", "sparkuser") \
            .option("password", "sparkpass") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        logging.info("DataFrame created successfully.")
        return raw_df
    except Exception as e:
        logging.error(f"Error reading from database: {str(e)}")
        return None

def write_table(dataframe, tableName):
    """Save data on database"""
    try:
        dataframe.write.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/sparkdb") \
            .option("dbtable", f"{tableName}") \
            .option("user", "sparkuser") \
            .option("password", "sparkpass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        logging.info(f"Data from table {tableName} persisted successfully.")
    except Exception as e:
        logging.error(f"Error persisting data: {e}")
        return None


def appendDataOnTable(dataframe, tableName):
    """Save data on database"""
    try:
        dataframe.write.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/sparkdb") \
            .option("dbtable", f"{tableName}") \
            .option("user", "sparkuser") \
            .option("password", "sparkpass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logging.info(f"Data from table {tableName} persisted successfully.")
    except Exception as e:
        logging.error(f"Error persisting data: {e}")
        return None