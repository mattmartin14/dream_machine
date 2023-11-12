#!/bin/bash

curl --progress-bar --location --output "spark.tgz" \
    "https://dlcdn.apache.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
tar xzf "spark.tgz" -C /usr/local --owner root --group root --no-same-owner && \
rm "spark.tgz"


