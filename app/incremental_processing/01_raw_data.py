from datetime import datetime
from utils.spark_connection import spark_conn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import sys
sys.path.append('/opt/bitnami/spark/app')

try:
    # Print available JARs for debugging
    print("Available JARs:", spark_conn.sparkContext.getConf().get("spark.jars"))
    
    # Read table data raw_people
    raw_df = spark_conn.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/sparkdb") \
        .option("dbtable", "raw_people") \
        .option("user", "sparkuser") \
        .option("password", "sparkpass") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    logging.info("DataFrame created successfully.")
    
    # Print schema and data
    logging.info("DataFrame Schema:")
    raw_df.printSchema()
    
    logging.info("DataFrame Content:")
    raw_df.show(20, truncate=False)

except Exception as e:
    logging.error(f"Error reading from database: {str(e)}")
    logging.error(f"Error type: {type(e).__name__}")
    raise
