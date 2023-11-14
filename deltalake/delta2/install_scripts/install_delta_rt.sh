#!/bin/bash

DELTA_SPARK_RT="delta-spark_2.12-3.0.0.jar"
#https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spar
DELTA_SPARK_URL="https://search.maven.org/remotecontent?filepath=io/delta/delta-spark_2.12/3.0.0/${DELTA_SPARK_RT}"
#ECHO $DELTA_SPARK_URL
#curl -Lo "$HOME/test_jars/${DELTA_SPARK_RT}" ${DELTA_SPARK_URL}
#chmod +x "./${DELTA_SPARK_RT}"

# for some reason these are not downloading and saving to the spark jars folder
curl -Lo "/usr/local/spark/jars/${DELTA_SPARK_RT}" ${DELTA_SPARK_URL}
chmod +x "/usr/local/spark/jars/${DELTA_SPARK_RT}"

# DELTA_CORE_RT="delta-core_2.12-2.4.0.jar"
# DELTA_CORE_URL="https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/${DELTA_CORE_RT}"
# curl -Lo "/usr/local/spark/jars/${DELTA_CORE_RT}" ${DELTA_CORE_URL}
# chmod +x "/usr/local/spark/jars/${DELTA_CORE_RT}"

# DELTA_STORAGE="delta-storage-3.0.0.jar"
# DELTA_STORAGE_URL="https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/${DELTA_STORAGE}"
# curl -Lo "/usr/local/spark/jars/${DELTA_STORAGE}" ${DELTA_STORAGE_URL}
# chmod +x "/usr/local/spark/jars/${DELTA_STORAGE}"

