import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from spark_connection import spark_conn


# data = os.path.join(os.path.dirname(__file__), '../datasets/fake_patient_visit_data.csv')
# data = os.path.abspath(data)

data = '/opt/bitnami/spark/app/datasets'


# Read streaming data from a socket
customSchema = (StructType() 
                    .add("PID", IntegerType(), True)
                    .add("Name", StringType(), True)
                    .add("DID", IntegerType(), True)
                    .add("DName", StringType(), True)
                    .add("VisitDate", DateType(), True)
            )
	
#read the CSV file with headers and apply the schema
dfPatients =  (spark_conn 
                        .readStream 
                        .format("csv")
                        .option("header", True)
                        .schema(customSchema)
                        .load(data)
                    )


#Apply filters to get only patients from the ortho department
orthoPatients = dfPatients.select("PID","Name").where("DID =86")

print(orthoPatients.isStreaming) # True
orthoPatients.printSchema()
orthoPatients.explain()

# Start the streaming query with output to console
query = orthoPatients \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("checkpointLocation", "/tmp/ortho_checkpoint") \
        .option("truncate", False) \
        .option("numRows", 33) \
        .start()


# Wait for the query to terminate
query.awaitTermination()