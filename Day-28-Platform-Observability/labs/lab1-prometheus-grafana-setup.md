# Lab 1 — Prometheus & Grafana Core Observability Setup

## Objective
Deploy a containerized Prometheus and Grafana stack, configure scrape jobs, and verify metric visualizer dashboards.

## Architecture
```
[Node Exporter :9100] ---> HTTP GET /metrics ---> [Prometheus :9090] ---> PromQL ---> [Grafana :3000]
```

## Step 1: Start the Docker Observability Stack
Navigate to the `docker/` directory and launch the compose stack:
```bash
cd docker
docker-compose up -d --build
```

## Step 2: Verify Container Health
Check the container status:
```bash
docker-compose ps
```

Verify that Prometheus (`:9090`), Grafana (`:3000`), Alertmanager (`:9093`), and Node Exporter (`:9100`) are running cleanly.

## Step 3: Access Web UIs
- **Prometheus Targets UI**: Open `http://localhost:9090/targets` in your browser. Verify `node_exporter` is in state `UP`.
- **Grafana Dashboard UI**: Open `http://localhost:3000` (Default credentials: `admin` / `admin`). Navigating to **Dashboards -> Hadoop Platform** will show the pre-loaded dashboards.

## Step 4: Run Verification Scripts
From the repository root, run:
```bash
bash scripts/verify-prometheus.sh
bash scripts/verify-grafana.sh
bash scripts/verify-exporters.sh
```
