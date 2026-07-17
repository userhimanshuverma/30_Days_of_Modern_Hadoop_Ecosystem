# Day 25: Apache Knox Security Gateway - Production-grade Dockerfile
FROM eclipse-temurin:11-jre-focal

# Maintainer info
LABEL maintainer="Antigravity Team"
LABEL description="Production-grade Apache Knox Security Gateway"

# Environment variables
ENV KNOX_VERSION=1.6.1
ENV KNOX_HOME=/opt/knox
ENV PATH=$PATH:$KNOX_HOME/bin

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    xmlstarlet \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and extract Apache Knox
RUN mkdir -p /opt && \
    curl -fSL https://archive.apache.org/dist/knox/${KNOX_VERSION}/knox-${KNOX_VERSION}.tar.gz -o /tmp/knox.tar.gz && \
    tar -xzf /tmp/knox.tar.gz -C /opt && \
    mv /opt/knox-${KNOX_VERSION} ${KNOX_HOME} && \
    rm /tmp/knox.tar.gz

# Create knox user and group for security isolation
RUN groupadd -r knox && useradd -r -g knox -d ${KNOX_HOME} knox

# Create necessary directories and set permissions
RUN mkdir -p ${KNOX_HOME}/data/security/keystores && \
    chown -R knox:knox ${KNOX_HOME}

# Set working directory
WORKDIR ${KNOX_HOME}

# Copy entrypoint and configurations
COPY --chown=knox:knox entrypoint.sh ${KNOX_HOME}/bin/entrypoint.sh
RUN chmod +x ${KNOX_HOME}/bin/entrypoint.sh

# Expose Knox Gateway default SSL port
EXPOSE 8443

# Switch to non-root knox user
USER knox

# Run the entrypoint script
ENTRYPOINT ["/opt/knox/bin/entrypoint.sh"]
