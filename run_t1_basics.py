import os
from time import time, sleep
from datetime import datetime
from utils.spark_connection import spark_conn
import logging
from pyspark.sql.functions import *

from app.t1_basics.gen_fake_records import  records_df
from app.t1_basics.gen_churn_records import churn_records_df
from app.t1_basics.churn_1 import extract_source_data, save_to_sink,read_job_config, parse_known_cmd_args, transform