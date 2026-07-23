# docker/Prometheus.Dockerfile
FROM prom/prometheus:v2.48.0

LABEL maintainer="SRE Team <sre@platform.org>"
LABEL description="Custom Prometheus image pre-configured for Hadoop Ecosystem observability"

COPY prometheus/prometheus.yml /etc/prometheus/prometheus.yml
COPY prometheus/recording-rules.yml /etc/prometheus/recording-rules.yml
COPY prometheus/alert-rules.yml /etc/prometheus/alert-rules.yml

EXPOSE 9090
