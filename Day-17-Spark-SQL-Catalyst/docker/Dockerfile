# Day 17 — Spark SQL & Catalyst Optimizer Docker Environment
# Location: Day-17-Spark-SQL-Catalyst/docker/Dockerfile

FROM openjdk-11-jdk-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    maven \
    procps \
    openssh-server \
    openssh-client \
    rsync \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set Version Variables
ENV HADOOP_VERSION=3.3.6
ENV SPARK_VERSION=3.5.1

# Set Home Environments
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/local/openjdk-11

# Add components to PATH
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 1. Download and Install Hadoop
RUN echo "Downloading Apache Hadoop $HADOOP_VERSION..." && \
    curl -sSL https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz | tar -xz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME

# 2. Download and Install Spark (built with Hadoop 3)
RUN echo "Downloading Apache Spark $SPARK_VERSION..." && \
    curl -sSL https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz | tar -xz -C /opt && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME

# Install PySpark for Python hands-on exercises
RUN pip3 install --no-cache-dir pyspark==$SPARK_VERSION pandas

# 3. Create directories for data storage and logging
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/dfs/tmp /opt/spark/logs /opt/spark/work /workspace

# Setup passwordless SSH for Hadoop admin scripts
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Copy Bootstrap script
COPY bootstrap.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose Cluster Ports
# HDFS: 9870 (NameNode Web), 9000 (RPC), 9864 (DataNode Web)
# Spark Standalone Master: 7077 (Master RPC), 8080 (Master Web UI)
# Spark Standalone Worker: 8081 (Worker Web UI)
# Spark History Server: 18080 (Web UI)
# Spark Driver UI: 4040 (running application UI)
EXPOSE 9870 9000 9864 7077 8080 8081 18080 4040

WORKDIR /workspace

ENTRYPOINT ["/entrypoint.sh"]
CMD ["spark-client"]
