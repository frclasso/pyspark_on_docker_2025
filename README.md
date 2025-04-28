# Running PySpark on Docker 2025


# First things first
    $ mkdirs spark_on_docker/app/basics
    $ docker-compose up -d --build  (build or rebuild the application)
    $ docker-compose down -v             (force stop)
    $ docker-compose  build --no-cache   (build without cache)
    $ docker-compose up -d               (start/re-start)


# Listing files
    $ docker exec -it spark_on_docker-spark-master-1 ls -la /opt/spark/

# Check the logs content and getting the connection information
    $ docker logs spark_on_docker-spark-master-1

`WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable`

`INFO Utils: Successfully started service 'sparkMaster' on port 7077.`

`INFO Master: Starting Spark master at spark://172.19.0.2:7077`

`INFO Master: Running Spark version 3.5.5`

`INFO JettyUtils: Start Jetty 0.0.0.0:8080 for MasterUI`

`INFO Utils: Successfully started service 'MasterUI' on port 8080.`


# Execute the pyspark file using the following command

## Basic tutorial module
    $ docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/app/run_basics.py

## Incremental processing module
    $ docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/app/run_t3_incremental_processing.py

## Window Fucntions module
    $ docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/app/run_window_functions.py

## Run ETL module
    $ docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/app/run_t4_etl.py --config_file_name /opt/spark/app/app/t4_etl/src/job_config.json --env dev


## Checking database
    $ docker exec -it spark_on_docker-postgres psql -U sparkuser -d sparkdb

# =========================================================================================================================

# References:
- https://medium.com/towards-data-engineering/a-beginners-guide-to-incremental-data-processing-in-pyspark-58034302fb64
- https://medium.com/towards-data-engineering/getting-started-with-incremental-data-processing-in-pyspark-169b4aeda6b3
- https://medium.com/@mehmood9501/using-apache-spark-docker-containers-to-run-pyspark-programs-using-spark-submit-afd6da480e0f
- https://medium.com/@nomannayeem/pyspark-made-simple-from-basics-to-big-data-mastery-cb1d702968be
- https://medium.com/@yoloshe302/pyspark-tutorial-read-and-write-streaming-data-401ed3d860e7
- https://www.macrometa.com/event-stream-processing/spark-structured-streaming
- https://medium.com/@dhanashrisaner.30/pyspark-window-functions-820ce4046b0b
- https://medium.com/towards-data-engineering/write-pyspark-etl-application-like-a-pro-1bdd6694acf6