import os
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from utils.spark_connection import spark_conn

import sys
sys.path.append('/opt/bitnami/spark/app')

# Configure logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Check if file exists
file_path = '/opt/bitnami/spark/app/datasets/churn/churn1.csv'
if not os.path.exists(file_path):
    raise FileNotFoundError(f"CSV file not found at {file_path}")


"""
Project Scenario: Predicting Customer Churn

Imagine you work for a telecom company, and you want to predict which customers are likely to stop using your service (churn). 
You have a large dataset containing customer information, such as how long they’ve been using the service, 
their payment method, and their overall usage.

The goal of this project is to:

    Load the data into PySpark.
    Perform data cleaning and preprocessing.
    Build a machine learning model to predict customer churn.
    Evaluate the model and make predictions.
"""

try:
    # Log Spark configuration for debugging
    logger.info(f"Spark version: {spark_conn.version}")
    logger.info(f"Spark configurations: {spark_conn.sparkContext.getConf().getAll()}")
    logger.info(f"Reading CSV file from: {file_path}")
    
    # Read the CSV file
    data = spark_conn.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "DROPMALFORMED") \
        .csv(file_path)
    
    # Log schema and row count for verification
    logger.info("DataFrame Schema:")
    data.printSchema()
    logger.info(f"Row count: {data.count()}")
    
    # Show sample data
    logger.info("Sample data:")
    data.show(5)

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    raise

try:
    # Step 2: Data Cleaning and Preprocessing
    data_cleaned = data.dropna()

    # Convert categorical columns to numerical ones using StringIndexer
    indexer = StringIndexer(inputCol="PaymentMethod", outputCol="PaymentMethodIndex")
    data_indexed = indexer.fit(data_cleaned).transform(data_cleaned)

    # Show the processed data
    data_indexed.show(5)
except Exception as e:
    logger.error(f"An error occurred during data cleaning and preprocessing: {str(e)}")
    raise

try:
    # Step 3: Feature Engineering

    # Select the features and label column
    assembler = VectorAssembler(
        inputCols=["Tenure", "MonthlyCharges", "TotalCharges", "PaymentMethodIndex"],
        outputCol="features"
    )

    # Apply the assembler to the DataFrame
    data_prepared = assembler.transform(data_indexed)

    # Show the prepared data
    data_prepared.select("features", "Churn").show(5)
except Exception as e:
    logger.error(f"An error occurred during feature engineering: {str(e)}")
    raise

try:
    # Step 4: Build and Train the Model
    from pyspark.ml.classification import LogisticRegression

    # Initialize the Logistic Regression model
    lr = LogisticRegression(featuresCol="features", labelCol="Churn")

    # Train the model
    model = lr.fit(data_prepared)

    # Make predictions on the dataset
    predictions = model.transform(data_prepared)

    # Show the predictions
    predictions.select("Churn", "prediction", "probability").show(5)
except Exception as e:
    logger.error(f"An error occurred during model training and prediction: {str(e)}")
    raise

try:
    # Step 5: Evaluate the Model
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    # Initialize the evaluator
    evaluator = BinaryClassificationEvaluator(labelCol="Churn", metricName="areaUnderROC")

    # Evaluate the model
    roc_auc = evaluator.evaluate(predictions)
    print(f"Area under ROC curve: {roc_auc}")
except Exception as e:
    logger.error(f"An error occurred during model evaluation: {str(e)}")
    raise