## Running PySpark on Docker


## First things first
    $ mkdirs spark_on_docker/app/basics
    $ docker-compose down up -d --build  (build or rebuild the application)
    $ docker-compose up -d               (stop)
    $ docker-compose down -v             (force stop)


## Check the logs content and getting the connection information
    $ docker logs spark_on_docker-spark-master-1

`25/03/17 20:07:31 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable`

`25/03/17 20:07:32 INFO Utils: Successfully started service 'sparkMaster' on port 7077.`

`25/03/17 20:07:32 INFO Master: Starting Spark master at spark://172.19.0.2:7077`

`25/03/17 20:07:32 INFO Master: Running Spark version 3.5.5`

`25/03/17 20:07:33 INFO JettyUtils: Start Jetty 0.0.0.0:8080 for MasterUI`

`25/03/17 20:07:33 INFO Utils: Successfully started service 'MasterUI' on port 8080.`


# Now we need to execute the pyspark file using the following command
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/anyfilename.py

docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/app/basics/create_rdd.py