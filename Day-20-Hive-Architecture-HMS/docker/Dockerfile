# Day 20 — Apache Hive and Metastore Docker Environment
# Location: Day-20-Hive-Architecture-HMS/docker/Dockerfile

FROM eclipse-temurin:8-jdk

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    procps \
    netcat-openbsd \
    postgresql-client \
    openssh-server \
    openssh-client \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Set Version Variables
ENV HADOOP_VERSION=3.3.6
ENV HIVE_VERSION=3.1.3

# Set Home Environments
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV JAVA_HOME=/opt/java/openjdk

# Add components to PATH
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin

# 1. Download and Install Hadoop
RUN echo "Downloading Apache Hadoop $HADOOP_VERSION..." && \
    curl -sSL https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz | tar -xz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME

# 2. Download and Install Hive
RUN echo "Downloading Apache Hive $HIVE_VERSION..." && \
    curl -sSL https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz | tar -xz -C /opt && \
    mv /opt/apache-hive-$HIVE_VERSION-bin $HIVE_HOME

# 3. Resolve Hive-Hadoop Guava Conflict (CRITICAL production step)
# Hive 3.1.3 ships with Guava 19.0. Hadoop 3.3.6 ships with Guava 29.0-jre.
# Having Guava 19.0 in Hive's classpath causes a NoSuchMethodError when communicating with Hadoop.
RUN rm -f $HIVE_HOME/lib/guava-19.0.jar && \
    cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/

# 4. Download and Install PostgreSQL JDBC Driver (Required for Metastore DB)
RUN echo "Downloading PostgreSQL JDBC Driver..." && \
    curl -sSL https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o $HIVE_HOME/lib/postgresql-42.6.0.jar

# 5. Create directories for data storage and logging
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/dfs/tmp /var/log/hive /workspace

# Setup passwordless SSH for Hadoop admin scripts (if needed)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Copy Bootstrap script
COPY bootstrap.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose Cluster Ports
# HDFS: 9870 (NameNode Web), 9000 (RPC), 9864 (DataNode Web)
# Hive Metastore (HMS): 9083 (Thrift)
# HiveServer2 (HS2): 10000 (Thrift/JDBC Client), 10002 (Web UI)
EXPOSE 9870 9000 9864 9083 10000 10002

WORKDIR /workspace

ENTRYPOINT ["/entrypoint.sh"]
CMD ["hiveserver2"]
