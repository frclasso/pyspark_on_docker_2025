from datetime import datetime
import logging
from pyspark.sql.functions import *
from read_write_data import write_table, read_table, appendDataOnTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
    Updated Raw Data at Time T1
    Assume that you did the previous aggregation at time t=T0, where To=2022–08–10 12:00:00. 
    At time t=T1 which is after a week, the raw data table will get a few new records. 
    Here is the code for creating a dataframe with existing and new order records.
"""

updated_raw_data = [
    ("ACC001", "ADD001", "ORD001", datetime(2020, 9, 14, 12, 0)),
    ("ACC001", "ADD001", "ORD002", datetime(2019, 8, 19, 12, 0)),
    ("ACC001", "ADD001", "ORD003", datetime(2018, 5, 23, 12, 0)),
    ("ACC001", "ADD002", "ORD004", datetime(2020, 3, 29, 12, 0)),
    ("ACC001", "ADD002", "ORD005", datetime(2020, 5, 18, 12, 0)),
    ("ACC001", "ADD003", "ORD006", datetime(2022, 2, 11, 12, 0)),
    ("ACC002", "ADD011", "ORD007", datetime(2022, 8, 10, 12, 0)),
    ("ACC002", "ADD011", "ORD008", datetime(2020, 1, 9, 12, 0)),
    ("ACC002", "ADD012", "ORD009", datetime(2019, 9, 8, 12, 0)),
    ("ACC002", "ADD011", "ORD010", datetime(2018, 3, 2, 12, 0)),
    ("ACC002", "ADD013", "ORD011", datetime(2021, 4, 5, 12, 0)),
    ("ACC003", "ADD021", "ORD012", datetime(2020, 2, 2, 12, 0)),
    ("ACC003", "ADD021", "ORD013", datetime(2019, 5, 1, 12, 0)),
    ("ACC003", "ADD022", "ORD014", datetime(2018, 7, 12, 12, 0)),
    ("ACC003", "ADD021", "ORD015", datetime(2020, 2, 10, 12, 0)),
    ("ACC003", "ADD023", "ORD016", datetime(2020, 9, 11, 12, 0)),
    ("ACC001", "ADD001", "ORD017", datetime(2022, 11, 20, 12, 0)),
    ("ACC001", "ADD002", "ORD019", datetime(2022, 12, 21, 12, 0)),
    ("ACC001", "ADD002", "ORD019", datetime(2022, 12, 21, 12, 15)),
    ("ACC002", "ADD011", "ORD021", datetime(2022, 10, 23, 12, 0)),
    ("ACC002", "ADD012", "ORD023", datetime(2022, 11, 1, 12, 0)),
    ("ACC003", "ADD022", "ORD025", datetime(2022, 12, 13, 12, 0)),
    ("ACC003", "ADD023", "ORD026", datetime(2022, 11, 12, 12, 0)),
    ("ACC003", "ADD022", "ORD027", datetime(2022, 10, 22, 12, 0)),
    ("ACC003", "ADD022", "ORD028", datetime(2022, 11, 2, 12, 0)),
]
Columns = ["account_id","address_id", "order_id", "delivered_order_time"]


# Create a SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark_conn = (SparkSession.builder
               .appName("IncrementalDataProcessing")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .config("spark.kryoserializer.buffer.max", "512m")
               .config("spark.eventLog.enabled", "true")
               .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") 
               .getOrCreate()
            )


updated_raw_df = spark_conn.createDataFrame(data=updated_raw_data, schema=Columns)

def appendData(dataframe, tableName):
    """Append new data to the existing raw data"""
    try:
        # Read the existing raw data
        existing_raw_df = read_table(spark_conn, tableName)
        logging.info(f">>>>>>>>>Previous numbers of rows:{dataframe.count()}")
        # Append the new data to the existing raw data
        new_updated_raw_df = existing_raw_df.union(dataframe)
        logging.info(f">>>>>>>>>New rows to insert:{new_updated_raw_df.count()}")
        # Write the updated raw data back to the table
        appendDataOnTable(new_updated_raw_df, tableName)
        logger.info(f"Data appended  on table {tableName} successfully.")
    except Exception as e:
        logger.error(f"Error appending data {tableName}: {e}")


tableName="raw_people"
appendData(updated_raw_df, tableName)

updated_people =  read_table(spark_conn, tableName)
logging.info(f">>>>>>>>> After updating, the total of rows:{updated_people.count()}!")
# TempView
updated_people.createOrReplaceTempView("view_updated_people")
updated_people.show()
