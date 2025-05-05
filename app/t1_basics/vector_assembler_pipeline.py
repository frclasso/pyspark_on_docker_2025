from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructType, StructField
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)

# Create Spark session with specific configurations
# spark = SparkSession.builder \
#     .appName("VectorAssemblerPipeline") \
#     .master("spark://spark-master:7077") \
#     .config("spark.driver.memory", "1g") \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .config("spark.kryoserializer.buffer.max", "512m") \
#     .config("spark.driver.bindAddress", "0.0.0.0") \
#     .config("spark.driver.host", "spark-master") \
#     .getOrCreate()

# Define schema for the data
vector_schema = StructType([
    StructField("label", DoubleType(), False),
    StructField("feature1", DoubleType(), False),
    StructField("feature2", DoubleType(), False)
])

# Sample data with explicit double values
vector_data = [(0.0, 1.0, 0.5), (1.0, 2.0, 1.5), (0.0, 0.5, 0.3), (1.0, 2.5, 1.7)]

# Assemble features into a vector
assembler = VectorAssembler(
    inputCols=["feature1", "feature2"],
    outputCol="features"
)

# Define a Logistic Regression model
logistc_regression = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=10,
    regParam=0.01
)

# Create a pipeline with the assembler and the logistic regression model
pipeline = Pipeline(stages=[assembler, logistc_regression])

def predictModel(dataframe, spark):
    """
    Function to predict using the trained model.
    :param dataframe: PySpark DataFrame containing the features.
    :param spark: SparkSession object.
    :return: DataFrame with predictions.
    """
    try:
        # Use the DataFrame as-is (do not recreate it)
        df = dataframe

        # Train the model
        model = pipeline.fit(df)

        # Make predictions
        predictions = model.transform(df)

        print("Show the predictions ============================================")
        predictions.select("label", "features", "prediction").show(truncate=False)
        logging.info("Pipeline execution completed successfully.")

        logging.info(f"Spark version: {spark.version}")
        logging.info(f"Spark configurations: {spark.sparkContext.getConf().getAll()}")
        executor_ids = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keys()
        logging.info(f"Available executors: {list(executor_ids)}")
        return predictions
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return None