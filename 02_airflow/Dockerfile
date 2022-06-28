FROM apache/airflow:2.3.2

ENV AIRFLOW_HOME=/opt/airflow

USER root

# git gcc g++ -qqq

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root
WORKDIR $AIRFLOW_HOME

# Spark
#ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}}/python:${PATH}"
ARG SPARK_VERSION="3.2.1"
ARG HADOOP_VERSION="3.2"

RUN mkdir -p spark && cd spark && \
    curl -O https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz && \
    tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz && \ 
    rm openjdk-11.0.2_linux-x64_bin.tar.gz


ENV JAVA_HOME "${AIRFLOW_HOME}/spark/jdk-11.0.2"
ENV PATH "${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p spark &&  \
    cd spark && \
    curl -O "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"  && \
    tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

ENV SPARK_HOME "${AIRFLOW_HOME}/spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

ENV PATH "${SPARK_HOME}/bin:${PATH}"

ENV PYTHONPATH "${SPARK_HOME}/python/:$PYTHONPATH"


RUN cd "${SPARK_HOME}/jars" && \
    curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar


# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
RUN apt-get update -qq && apt-get install vim -qqq
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Google Cloud
ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME /home/google-cloud-sdk

ENV PATH "${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

USER $AIRFLOW_UID