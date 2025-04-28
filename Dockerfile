FROM apache/spark:3.5.0

# Update SPARK_HOME to match the default path in the base image
ENV SPARK_HOME=/opt/spark \
    PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

WORKDIR ${SPARK_HOME}

USER root

RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file and install dependencies
COPY requirements.txt ${SPARK_HOME}/app/requirements.txt
COPY . ${SPARK_HOME}/app/
COPY app/t4_etl/datasets /opt/spark/app/t4_etl/datasets

# Add this line before the CMD instruction
RUN curl -o ${SPARK_HOME}/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

RUN pip3 install --no-cache-dir -r ${SPARK_HOME}/app/requirements.txt && \
    pip3 install --upgrade pip && \
    rm -rf ~/.cache/pip/*

# Expose Spark ports
EXPOSE 4040 7077 8080

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]