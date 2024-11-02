#!/usr/bin/env bash

if [[ -f .env ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
      eval "export $line"
  done < ../.env
else
  export SPARK_HOME=/opt/spark
  export SPARK_MASTER_HOST=127.0.0.1
  export SPARK_MASTER_PORT=7077
  export SPARK_MASTER="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
  export SPARK_VERSION=3.5.3
  export SCALA_VERSION=2.13
  export HADOOP_VERSION=3
fi

if [ ! -d /opt/spark ]; then
  echo "Installing spark..."
  wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz" \
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz
  export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
fi


