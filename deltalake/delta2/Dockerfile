
## Author: Matt Martin
## Date: 2023-11-06
## Desc: Docker Container for Delta Lake


ARG REGISTRY=quay.io
ARG OWNER=jupyter
ARG BASE_CONTAINER=$REGISTRY/$OWNER/scipy-notebook
FROM $BASE_CONTAINER

LABEL maintainer="Matt Martin"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

## add install scripts to docker folder
RUN mkdir -p /install_scripts 
COPY ./install_scripts /install_scripts
RUN chmod +x /install_scripts/*

## Install Open JDK 17
#RUN /install_scripts/install_open_jdk_17.sh
RUN apt-get update --yes \
    && apt-get install --yes --no-install-recommends "openjdk-17-jre-headless" \
    ca-certificates-java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


## install Spark
ENV APACHE_SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

#RUN /install_scripts/install_spark.sh
RUN curl --progress-bar --location --output "spark.tgz" \
    "https://dlcdn.apache.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar xzf "spark.tgz" -C /usr/local --owner root --group root --no-same-owner \
    && rm "spark.tgz"

# Configure Spark
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH="${PATH}:${SPARK_HOME}/bin"
RUN ln -s "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}"
RUN ln -s "${SPARK_HOME}/sbin/spark-config.sh" /usr/local/bin/before-notebook.d/spark-config.sh

# # add Delta Spark Runtime
#RUN /install_scripts/install_delta_rt.sh
## note: after trying several times over, delta spark just calls ivy to install the runtimes
ARG DELTA_SPARK_RT="delta-spark_2.12-3.0.0.jar"
ARG DELTA_SPARK_URL="https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/${DELTA_SPARK_RT}"
RUN curl -o "${SPARK_HOME}/jars/${DELTA_SPARK_RT}" ${DELTA_SPARK_URL} \
    && chmod +x "${SPARK_HOME}/jars/${DELTA_SPARK_RT}"

#add the spark defaults config file
COPY spark-defaults.conf /usr/local/spark/conf


WORKDIR /tmp

# Configure IPython system-wide
COPY ipython_kernel_config.py "/etc/ipython/"
RUN fix-permissions "/etc/ipython/"

USER ${NB_UID}
RUN pip install pyspark==3.5 
RUN pip install delta-spark==3.0.0
    
RUN mkdir -p /home/jovyan/deltahouse

RUN fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

#add example delta python file
COPY delta_lake_template.ipynb "${HOME}"


WORKDIR "${HOME}"
EXPOSE 4040