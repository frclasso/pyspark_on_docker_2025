FROM apache/spark:3.5.0

ENV SPARK_HOME=/opt/spark \
    PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

WORKDIR ${SPARK_HOME}

USER root

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file and install dependencies
COPY requirements.txt ${SPARK_HOME}/
RUN pip3 install --no-cache-dir -r requirements.txt && \
    pip3 install --upgrade pip && \
    rm -rf ~/.cache/pip/*

# Copy the application code
COPY . ${SPARK_HOME}/app/

# Expose Spark ports
EXPOSE 4040 7077 8080

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]
