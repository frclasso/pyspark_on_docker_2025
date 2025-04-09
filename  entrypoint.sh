#!/bin/bash

if [ "$SPARK_MODE" == "master" ]; then
    exec /opt/spark/sbin/start-master.sh --host spark-master
elif [ "$SPARK_MODE" == "worker" ]; then
    exec /opt/spark/sbin/start-worker.sh spark://spark-master:7077
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi

tail -f /dev/null
