from datetime import datetime
from utils.spark_connection import spark_conn

import sys
sys.path.append('/opt/bitnami/spark/app')

raw_data = [
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
]

Columns = ["account_id", "address_id", "order_id", "delivered_order_time"]

raw_df = spark_conn.createDataFrame(data=raw_data, schema=Columns)
raw_df.printSchema()
raw_df.show(truncate=False)

