from time import time, sleep
from datetime import datetime
from utils.spark_connection import spark_conn
import logging

from app.t4_etl.src.utils import extract_source_data, save_to_sink,read_job_config, parse_known_cmd_args
from app.t4_etl.src.transform import transform

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


args = parse_known_cmd_args()
config = read_job_config(args.config_file_name)


# Extract
df_dict = extract_source_data(spark_conn, config["source"])
logger.info("Extracted data from source =========================================")
print(df_dict)

# Transform
df_transformed = transform(df_dict)
logger.info("Transformed data ====================================================")
print(df_transformed.head(5))

# Load
save_to_sink(df_transformed, config["sink"], args.env)
logger.info("Saving data to sink ================================================")


"""
References:
https://medium.com/towards-data-engineering/write-pyspark-etl-application-like-a-pro-1bdd6694acf6
"""