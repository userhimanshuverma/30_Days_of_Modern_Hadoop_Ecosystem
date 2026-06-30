# Dockerfile
# Production-grade customized Apache ZooKeeper image

FROM zookeeper:3.8.4

# Install troubleshooting utilities commonly missing in standard images
# - netcat-openbsd / netcat: for ZK 4-letter word commands (e.g. echo stat | nc localhost 2181)
# - iproute2: for network simulation (e.g. tc to simulate network latency/partitions)
# - curl: for healthcheck endpoint calls and API interactions
# - procps: for ps and top diagnostics
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    iproute2 \
    curl \
    dnsutils \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Revert to zookeeper user for security
USER zookeeper

# Expose default ZooKeeper ports:
# - 2181: Client connections
# - 2888: Peer-to-peer quorum communication (follower-to-leader synchronization)
# - 3888: Leader election
# - 8080: AdminServer HTTP API
# - 7000: Prometheus Metrics
EXPOSE 2181 2888 3888 8080 7000
