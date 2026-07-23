# Lab 2 — Attaching JMX Exporters to Kafka & HDFS Services

## Objective
Learn how to instrument Java virtual machines (JVMs) running Hadoop NameNode and Kafka Brokers using the Prometheus JMX Java Agent.

## Step 1: Download the Prometheus JMX Javaagent Jar
```bash
mkdir -p /opt/jmx_exporter
curl -L https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
  -o /opt/jmx_exporter/jmx_prometheus_javaagent.jar
```

## Step 2: Configure Kafka Broker JMX Export
Copy `exporters/jmx_exporter/kafka.yml` into `/opt/jmx_exporter/kafka.yml`.

Set the JVM options before starting Kafka:
```bash
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=5558:/opt/jmx_exporter/kafka.yml"

bin/kafka-server-start.sh config/server.properties
```

## Step 3: Verify Exposed Metrics
Test HTTP GET endpoint on port 5558:
```bash
curl -s http://localhost:5558/metrics | grep kafka_server_brokertopicmetrics
```

Expected output:
```text
kafka_server_brokertopicmetrics_messagesinpersec_count{topic="orders"} 142050
```
