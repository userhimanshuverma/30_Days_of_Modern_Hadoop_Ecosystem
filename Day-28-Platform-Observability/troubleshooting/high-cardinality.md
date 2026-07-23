# Troubleshooting Playbook: High Cardinality & TSDB Memory Explosion

## Symptoms
- Prometheus TSDB process consumes excessive RAM and crashes with Out Of Memory (`OOMKilled`).
- PromQL queries run extremely slowly or time out.

## Root Causes
Dynamically generated high-cardinality label values (e.g. including UUIDs, user IDs, timestamps, or full SQL queries in metric labels).

## Diagnostic Steps

### 1. Identify Top High-Cardinality Metrics
Execute PromQL query in Prometheus UI:
```promql
topk(10, count by (__name__) ({__name__=~".+"}))
```

### 2. Inspect TSDB Head Block Statistics
Run Prometheus TSDB status API query:
```bash
curl -s http://localhost:9090/api/v1/status/tsdb | jq .
```
Examine `headStats.numSeries` and `labelValueCountByLabelName`.

## Remediation Steps

### Add Relabeling Drop Rules in `prometheus.yml`
Drop volatile labels before indexing into memory:
```yaml
scrape_configs:
  - job_name: 'spark_metrics'
    metric_relabel_configs:
      - source_labels: [user_id]
        action: labeldrop
      - source_labels: [task_attempt_id]
        action: labeldrop
```
Restart or reload Prometheus configuration (`curl -X POST http://localhost:9090/-/reload`).
