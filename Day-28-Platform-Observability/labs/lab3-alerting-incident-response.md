# Lab 3 — Alerting Rules & Incident Response Simulation

## Objective
Simulate a synthetic failure (e.g. disk space pressure or missing target), observe rule firing in Prometheus, inspect Alertmanager routing, and confirm alert suppression.

## Step 1: Inspect Active Alert Rules
Open `http://localhost:9090/alerts` in Prometheus UI to view configured alert rules (`HostDiskSpaceFillingFast`, `PrometheusTargetMissing`, `KafkaConsumerGroupLagHigh`).

## Step 2: Trigger Synthetic Scrape Failure
Stop the Node Exporter container to trigger `PrometheusTargetMissing`:
```bash
docker stop day28-node-exporter
```

## Step 3: Observe Alert State Progression
1. **Pending**: Prometheus detects `up == 0` for target `node-exporter:9100`. State switches to `PENDING` for 3 minutes (`for: 3m`).
2. **Firing**: After 3 minutes, state switches to `FIRING`.
3. **Alertmanager Push**: Prometheus POSTs alert payload to Alertmanager at `http://alertmanager:9093`.

## Step 4: Verify Alertmanager Notification
Open `http://localhost:9093` to see the firing alert grouped under `tier=observability`.

## Step 5: Remediate Incident
Restart the Node Exporter:
```bash
docker start day28-node-exporter
```
Prometheus will send a `RESOLVED` payload automatically.
