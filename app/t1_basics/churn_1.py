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

def cleaning_data(data):
    try:
        # Step 2: Data Cleaning and Preprocessing
        data_cleaned = data.dropna()

        # Convert categorical columns to numerical ones using StringIndexer
        indexer = StringIndexer(inputCol="PaymentMethod", outputCol="PaymentMethodIndex")
        data_indexed = indexer.fit(data_cleaned).transform(data_cleaned)

        return data_indexed
    except Exception as e:
        logger.error(f"An error occurred during data cleaning and preprocessing: {str(e)}")
        raise


def churn_feature_engineering(data_indexed):
    try:
        # Step 3: Feature Engineering
        assembler = VectorAssembler(
            inputCols=["Tenure", "MonthlyCharges", "TotalCharges", "PaymentMethodIndex"],
            outputCol="features"
        )

        # Apply the assembler to the DataFrame
        data_prepared = assembler.transform(data_indexed)
        return data_prepared
    except Exception as e:
        logger.error(f"An error occurred during feature engineering: {str(e)}")
        raise


def churn_model(data):
    try:
        # Step 1: Load the Data
        # Step 2: Clean the Data
        data_cleaned = cleaning_data(data)

        # Step 3: Feature Engineering
        data_prepared = churn_feature_engineering(data_cleaned)

         # Step 4: Build and Train the Model
        from pyspark.ml.classification import LogisticRegression

        # Initialize the Logistic Regression model
        lr = LogisticRegression(featuresCol="features", labelCol="Churn")

        # Train the model
        model = lr.fit(data_prepared)

        # Make predictions on the dataset
        predictions = model.transform(data_prepared)

        return predictions

    except Exception as e:
        logger.error(f"An error occurred in the churn model pipeline: {str(e)}")
        raise


def evaluate_model(predictions):
    try:
        # Step 5: Evaluate the Model
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        # Initialize the evaluator
        evaluator = BinaryClassificationEvaluator(labelCol="Churn", metricName="areaUnderROC")

        # Evaluate the model
        roc_auc = evaluator.evaluate(predictions)
        logger.info(f"Area under ROC curve: {roc_auc}")
        return roc_auc
    except Exception as e:
        logger.error(f"An error occurred during model evaluation: {str(e)}")
        raise

