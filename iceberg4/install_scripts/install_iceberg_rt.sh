#!/bin/bash

ICE_RT="iceberg-spark-runtime-${ICEBERG_MAJOR_VERSION}_2.12-${ICEBERG_MINOR_VERSION}.jar"
curl -Lo "/usr/local/spark/jars/${ICE_RT}" \
    "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_MAJOR_VERSION}_2.12/${ICEBERG_MINOR_VERSION}/${ICE_RT}"

chmod +x "/usr/local/spark/jars/${ICE_RT}"
