# docker/Dockerfile
# Custom Production-Grade Apache Airflow Image
# Extends standard apache/airflow to support HDFS, Spark, Kafka, and Hive clients

FROM apache/airflow:2.7.2-python3.10

USER root

# Install system dependencies (Java JDK, client utilities, compilers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    git \
    wget \
    curl \
    gnupg \
    procps \
    unzip \
    build-essential \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Environment Variables for Java and Apache Spark client integrations
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3.3.6

# Download and install Spark and Hadoop configurations for YARN client usage
WORKDIR /opt
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Setup dummy hadoop configuration directories
RUN mkdir -p /etc/hadoop/conf && \
    mkdir -p /opt/airflow/dags && \
    mkdir -p /opt/airflow/logs && \
    mkdir -p /opt/airflow/plugins && \
    chown -R airflow:root /opt/airflow /etc/hadoop/conf /opt/spark

# Switch back to the airflow user for secure execution
USER airflow

# Install Python packages for Airflow integrations (Hadoop ecosystem)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-hive \
    apache-airflow-providers-postgres \
    apache-airflow-providers-redis \
    psycopg2-binary \
    confluent-kafka \
    pyspark==3.4.1

WORKDIR /opt/airflow
