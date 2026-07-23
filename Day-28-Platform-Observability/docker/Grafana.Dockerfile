# docker/Grafana.Dockerfile
FROM grafana/grafana:10.2.0

LABEL maintainer="SRE Team <sre@platform.org>"
LABEL description="Custom Grafana image with pre-provisioned data sources and dashboards"

COPY grafana/provisioning /etc/grafana/provisioning
COPY dashboards /var/lib/grafana/dashboards

EXPOSE 3000
