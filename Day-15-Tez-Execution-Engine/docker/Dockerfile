# Production-grade Apache Tez + Hive + Hadoop Environment
FROM openjdk-8-jdk-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    maven \
    procps \
    openssh-server \
    openssh-client \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Set Version Variables
ENV HADOOP_VERSION=3.3.6
ENV TEZ_VERSION=0.10.2
ENV HIVE_VERSION=3.1.3

# Set Home Environments
ENV HADOOP_HOME=/opt/hadoop
ENV TEZ_HOME=/opt/tez
ENV HIVE_HOME=/opt/hive
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Add components to PATH
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$TEZ_HOME/bin

# 1. Download and Install Hadoop
RUN echo "Downloading Apache Hadoop $HADOOP_VERSION..." && \
    curl -sSL https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz | tar -xz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME

# 2. Download and Install Apache Tez
RUN echo "Downloading Apache Tez $TEZ_VERSION..." && \
    curl -sSL https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz | tar -xz -C /opt && \
    mv /opt/apache-tez-$TEZ_VERSION-bin $TEZ_HOME

# 3. Download and Install Apache Hive
RUN echo "Downloading Apache Hive $HIVE_VERSION..." && \
    curl -sSL https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz | tar -xz -C /opt && \
    mv /opt/apache-hive-$HIVE_VERSION-bin $HIVE_HOME

# 4. Resolve Hive vs Hadoop Guava Conflict (Critical step for Hive 3 + Hadoop 3)
RUN rm $HIVE_HOME/lib/guava-19.0.jar && \
    cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/

# 5. Configure Apache Tez Jars & Tarball
# Tez requires a tarball of its libraries to be located in HDFS for distributed task execution.
# We create a tarball of Tez's folder if it isn't packaged, but apache-tez-bin already has a tarball inside share/tez.tar.gz or we compress it.
# We compress the lib directories so YARN containers can retrieve them.
RUN cd $TEZ_HOME && tar -czf tez-$TEZ_VERSION.tar.gz lib/ *.jar

# 6. Create directories for data storage
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/dfs/tmp /opt/tez/conf /var/lib/hive

# Setup passwordless SSH for Hadoop admin scripts
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Copy Bootstrap script
COPY bootstrap.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose Cluster Ports
# HDFS: 9870 (NameNode Web), 9000 (RPC), 9864 (DataNode Web)
# YARN: 8088 (ResourceManager Web), 8042 (NodeManager Web)
# Hive: 10000 (HiveServer2 thrift), 10002 (HiveServer2 Web UI)
EXPOSE 9870 9000 9864 8088 8032 8042 10000 10002

WORKDIR /workspace

ENTRYPOINT ["/entrypoint.sh"]
CMD ["hiveserver2"]
