# Running PySpark on Docker 2025


# First things first
    $ mkdirs spark_on_docker/app/basics
    $ docker-compose down up -d --build  (build or rebuild the application)
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
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/app/main.py

# Basic tutorial
- ../opt/bitnami/spark/app/gen_fake_records.py
- ../opt/bitnami/spark/anyfilename.py
- .../opt/bitnami/spark/app/basics/create_rdd.py
- ../opt/bitnami/spark/app/basics/dataframe_join.py
- ../opt/bitnami/spark/app/basics/create_dataframe.py
- ../opt/bitnami/spark/app/basics/ranking.py
- ../opt/bitnami/spark/app/basics/read_stream.py
- ../opt/bitnami/spark/app/basics/churn_1.py

# Incremental processing methods


## Checking database
- docker exec -it spark_on_docker-postgres psql -U sparkuser -d sparkdb

# References:
- https://medium.com/towards-data-engineering/a-beginners-guide-to-incremental-data-processing-in-pyspark-58034302fb64
- https://medium.com/towards-data-engineering/getting-started-with-incremental-data-processing-in-pyspark-169b4aeda6b3
- https://medium.com/@mehmood9501/using-apache-spark-docker-containers-to-run-pyspark-programs-using-spark-submit-afd6da480e0f
- https://medium.com/@nomannayeem/pyspark-made-simple-from-basics-to-big-data-mastery-cb1d702968be
- https://medium.com/@yoloshe302/pyspark-tutorial-read-and-write-streaming-data-401ed3d860e7
- https://www.macrometa.com/event-stream-processing/spark-structured-streaming