from time import time, sleep
from datetime import datetime
from utils.spark_connection import spark_conn
import logging
from pyspark.sql.functions import *
from app.t2_incremental_processing.read_write_data import write_table, read_table, appendDataOnTable, createDataframe
from app.t2_incremental_processing.gen_people_raw_data import people_raw_data, people_columns
from app.t2_incremental_processing.updating_data import appendData, updated_raw_df


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GENERATE AND INGEST RAW DATA
def insertInitailData(tableName, data, schema) ->None:
    try:
        raw_df = createDataframe(spark_conn, data, schema)
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

# Ingesting initial data
tableName = "raw_people"
data = people_raw_data
schema = people_columns
insertInitailData(tableName, data, schema)

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
            names.append(table.name)
        return names
    except Exception as e:
        logger.error(f"Error listing tables: {e}")


# Tempviews
raw_people_df.createOrReplaceTempView("view_raw_people")

agregated_people_df.createOrReplaceTempView("view_agregated_people")

raw_people_df.show()
agregated_people_df.show()


# Waiting 10s
sleep(10)

# Appending data
appendData( spark_conn, dataframe=updated_raw_df, tableName="raw_people")

updated_people =  read_table(spark_conn, tableName)
logging.info(f">>>>>>>>> After updating, the total of rows:{updated_people.count()}!") #57

# TempView
updated_people.createOrReplaceTempView("view_updated_people")
updated_people.show()


# Find Delta Records
delta_df = updated_raw_df.filter(updated_raw_df.delivered_order_time > datetime(2022, 8, 10, 12, 0))
delta_df.show()    
logging.info(f">>>>>>>>> Getting filtered data")


# Aggregating Delta Records
delta_agg_df = (delta_df.groupBy("account_id", "address_id")
                        .agg(
                            count("order_id").alias("net_order_count"), 
                            max("delivered_order_time").alias("recent_order_delivered_time")
                            )
                )

delta_agg_df.show()
logging.info(f">>>>>>>>> Aggregating filtered data")


#Using the unionAll technique to combine the delta aggregate result with the historical aggregate result to get the desired result
peple_delta_agg_union = (delta_agg_df.unionAll(agregated_people_df)
                   .groupBy("account_id", "address_id")
                    .agg(
                        sum("net_order_count").alias("net_order_count"), 
                        max("recent_order_delivered_time").alias("recent_order_delivered_time")
                    )
                )

peple_delta_agg_union.show()
logging.info(f">>>>>>>>> Union of delta and agregated peple filtered data")

# Saving agregated data
save_aggdata = write_table(peple_delta_agg_union, "agregated_people")

# Listing tables
print(listTables(spark_conn))
print("Done")


# Reference
# https://medium.com/towards-data-engineering/a-beginners-guide-to-incremental-data-processing-in-pyspark-58034302fb64
# https://medium.com/towards-data-engineering/getting-started-with-incremental-data-processing-in-pyspark-169b4aeda6b3