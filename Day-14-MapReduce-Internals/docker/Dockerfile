# Dockerfile for Day 14 MapReduce Environment
FROM openjdk-11-jdk-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    maven \
    openssh-server \
    openssh-client \
    rsync \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Environment Variables
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download and install Hadoop
RUN curl -sSL https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz | tar -xz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME

# Copy configuration files placeholder (will be mounted via volume, but create directory)
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/dfs/tmp

# Setup passwordless SSH for Hadoop scripts
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Expose ports:
# NameNode: 9870 (Web), 9000 (RPC)
# DataNode: 9864 (Web)
# ResourceManager: 8088 (Web), 8032 (Scheduler)
# NodeManager: 8042 (Web)
# JobHistoryServer: 19888 (Web)
EXPOSE 9870 9000 9864 8088 8032 8042 19888 10020

WORKDIR /workspace

CMD ["bash"]
