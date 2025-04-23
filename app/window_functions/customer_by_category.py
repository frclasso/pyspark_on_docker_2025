import logging
from datetime import datetime
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, lag, lead, col, avg, sum, min, max, count
from pyspark.sql import Window
from pyspark.sql import DataFrame

# Defining the Window Specification
"""
    window functions are essential for performing calculations across a set of rows that are related to the current row.

    Unlike aggregate functions, window functions retain the original rows and compute results for subsets of data.

    Window functions operate over a subset of rows  and return a result for each row while retaining all rows. 
    These functions are powerful for analyzing trends, ranking data, and calculating cumulative statistics. 
"""
window_spec = Window.partitionBy("Category").orderBy("Amount")

def ApplyRanking(dataframe: DataFrame) -> DataFrame:
    """
        ** Key Functions
        row_number(): Sequentially assigns a unique number to each row.
        rank(): Assigns a rank to rows, with ties resulting in gaps.
        dense_rank(): Similar to rank() but without gaps in ranking.
        percent_rank(): Computes the relative rank of rows as a percentage.
        ntile(n): Divides rows into n approximately equal parts.
    """
    df_ranked = (
        dataframe.withColumn("row_number", row_number().over(window_spec))
                  .withColumn("rank", rank().over(window_spec))
                  .withColumn("dense_rank", dense_rank().over(window_spec))   
                 )
    return df_ranked

def ApplyLagLead(dataframe: DataFrame) -> DataFrame:
    """
        ** Key Functions
        lag(column, offset): Fetches the value from a previous row in the same window.
        lead(column, offset): Fetches the value from the next row in the same window.
    """
    df_trends = (
        dataframe.withColumn("previous_amount", lag("Amount", 1).over(window_spec))
                 .withColumn("next_amount", lead("Amount", 1).over(window_spec))
    )
    return df_trends


def ApplyAggregateFunctions(dataframe: DataFrame) -> DataFrame:
    """
        ** Key Functions
        avg(column): Computes the average of a column.
        sum(column): Computes the sum of a column.
        min(column): Finds the minimum value in a column.
        max(column): Finds the maximum value in a column.
        count(column): Counts the number of rows.
    """
    df_aggregates = (
        dataframe.withColumn("avg_amount", avg("Amount").over(window_spec))
                 .withColumn("total_amount", sum("Amount").over(window_spec))
                 .withColumn("min_amount", min("Amount").over(window_spec))
                 .withColumn("max_amount", max("Amount").over(window_spec))
                 .withColumn("count", count("Amount").over(window_spec))
    )
    return df_aggregates