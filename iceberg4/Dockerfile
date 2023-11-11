
## Author: Matt Martin
## Date: 2023-11-06
## Desc: Docker Container for Iceberg


ARG REGISTRY=quay.io
ARG OWNER=jupyter
ARG BASE_CONTAINER=$REGISTRY/$OWNER/scipy-notebook
FROM $BASE_CONTAINER

LABEL maintainer="Matt Martin"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

RUN mkdir -p /install_scripts 

## Install Open JDK 17
COPY install_open_jdk_17.sh /install_scripts
RUN chmod +x /install_scripts/install_open_jdk_17.sh && /install_scripts/install_open_jdk_17.sh

## install Spark
ENV APACHE_SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

COPY install_spark.sh /install_scripts
RUN chmod +x /install_scripts/install_spark.sh && /install_scripts/install_spark.sh

# Configure Spark
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH="${PATH}:${SPARK_HOME}/bin"
RUN ln -s "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}"
RUN ln -s "${SPARK_HOME}/sbin/spark-config.sh" /usr/local/bin/before-notebook.d/spark-config.sh

# add Iceberg Runtime
ENV ICEBERG_MAJOR_VERSION=3.5
ENV ICEBERG_MINOR_VERSION=1.4.2
COPY install_iceberg_rt.sh /install_scripts
RUN chmod +x /install_scripts/install_iceberg_rt.sh && /install_scripts/install_iceberg_rt.sh

#add the spark defaults config file
COPY spark-defaults.conf /usr/local/spark/conf

WORKDIR /tmp

# Configure IPython system-wide
COPY ipython_kernel_config.py "/etc/ipython/"
RUN fix-permissions "/etc/ipython/"

USER ${NB_UID}
RUN pip install pyspark==3.5
    
RUN mkdir -p /home/jovyan/icehouse

RUN fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

#add example delta python file
COPY iceberg_template.ipynb "${HOME}"

WORKDIR "${HOME}"
EXPOSE 4040