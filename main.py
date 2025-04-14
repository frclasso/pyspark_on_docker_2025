from datetime import datetime
from utils.spark_connection import spark_conn
import logging
from pyspark.sql.functions import *
from app.incremental_processing.gen_raw_data import generate_raw_data
from app.incremental_processing.read_write_data import write_table, read_table


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GENERATE AND INGEST RAW DATA
def insertInitailData(tableName) ->None:
    try:
        raw_df = generate_raw_data(spark_conn)
        if raw_df is not None:
            # raw_df.show(truncate=False)
            write_table(dataframe=raw_df, tableName=tableName) # persist
            logger.info("Data ingestion completed successfully.")
    except Exception as e:
        logger.error(f"Error ingesting data from {tableName}: {e}")


def readRawPeople(tableName) ->None:
    try:
        raw_people = read_table(spark_conn, tableName)
        if raw_people is not None:
            # raw_df.show(truncate=False)
            logger.info("Raw data reading completed successfully.")
            return raw_people
    except Exception as e:
        logger.error(f"Error reading {tableName} data: {e}")


def agregatingData(dataframe, tableName):
    """Agregate data"""
    try:
        agg_data = (dataframe.groupBy("account_id", "address_id")
                    .agg(count("order_id")
                    .alias("net_order_count"), max("delivered_order_time")
                    .alias("recent_order_delivered_time")
                    )
        )
        return agg_data
    except Exception as e:
        logger.error(f"Error agregating data from {tableName}: {e}")

tableName = "raw_people"
# Ingesting initial data
insertInitailData(tableName)

# Reading data
raw_people_df = readRawPeople(tableName)
print(f">>>>>>>>>>>>Numbers of inserted rows: {raw_people_df.count()}")
# raw_people_df.show(truncate=False)

# Agregating data
agregated_people_df = agregatingData(raw_people_df, tableName=tableName)
# agregated_people_df.show(truncate=False)

# Saving agregated data
save_aggdata = write_table(agregated_people_df, "agregated_people")


def listTables(spark_conn):
    """List all tables in the database"""
    try:
        names = []
        tables = spark_conn.catalog.listTables()
        if not tables:
            logger.warning("No tables found in the database.")
        for table in tables:
            # print(f"Table Name: {table.name}, Database: {table.database}, Is Temporary: {table.isTemporary}")
            names.append(table.name)
        return names
    except Exception as e:
        logger.error(f"Error listing tables: {e}")


# Tempviews
raw_people_df.createOrReplaceTempView("view_raw_people")

agregated_people_df.createOrReplaceTempView("view_agregated_people")

raw_people_df.show()
agregated_people_df.show()

print(listTables(spark_conn))

print("Done")

# Reference
# https://medium.com/towards-data-engineering/a-beginners-guide-to-incremental-data-processing-in-pyspark-58034302fb64