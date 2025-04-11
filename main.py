from datetime import datetime
from utils.spark_connection import spark_conn
import logging
from pyspark.sql.functions import *
from app.incremental_processing.gen_raw_data import generate_raw_data, persisting_data
from app.incremental_processing.read_table_raw_people import read_table_raw_people

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GENERATE AND INGEST RAW DATA
def insertInitailData() ->None:
    try:
        raw_df = generate_raw_data(spark_conn)
        if raw_df is not None:
            raw_schema = raw_df.printSchema()
            raw_df.show(truncate=False)
            persisting_data(raw_df)
            logger.info("Data ingestion completed successfully.")
    except Exception as e:
        logger.error(f"Error ingesting data: {e}")


def readRawPeople() ->None:
    try:
        raw_df = read_table_raw_people(spark_conn)
        if raw_df is not None:
            raw_df.show(truncate=False)
            logger.info("Data reading completed successfully.")
    except Exception as e:
        logger.error(f"Error reading data: {e}")

#ingest initial data
insertInitailData()

# Agregate data
raw_people = ()
agg_data = (raw_people.groupBy("account_id", "address_id")
            .agg(count("order_id")
            .alias("net_order_count"), max("delivered_order_time")
            .alias("recent_order_delivered_time")
            )
)

agg_data.show()


#refs
# https://medium.com/towards-data-engineering/a-beginners-guide-to-incremental-data-processing-in-pyspark-58034302fb64