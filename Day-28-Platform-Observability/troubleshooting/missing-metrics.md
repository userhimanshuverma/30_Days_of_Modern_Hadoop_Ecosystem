# Troubleshooting Playbook: Missing Metrics & Scrape Failures

## Symptoms
- Prometheus target status shows `DOWN` with error `connection refused` or `context deadline exceeded`.
- Grafana dashboard panels display "No Data".

## Root Causes
1. **Target Port Unreachable**: Network security group / firewall blocking scrape port (`9100`, `5558`, `4040`).
2. **JMX Exporter JavaAgent Not Bound**: JVM started without `-javaagent` argument.
3. **Scrape Timeout**: Exporter taking longer than `scrape_timeout: 10s` to serialize MBeans.
4. **Invalid Prometheus YAML Syntax**: Indentation errors in `prometheus.yml`.

## Diagnostic & Remediation Steps

### 1. Test Exporter Reachability
From the Prometheus server container/host, execute `curl`:
```bash
curl -i http://<target-ip>:5558/metrics
```
If connection times out, verify security groups and local OS iptables rules.

### 2. Inspect Prometheus Target Errors
Query the Prometheus API endpoint:
```bash
curl -s http://localhost:9090/api/v1/targets | grep -A 5 '"health":"down"'
```

### 3. Check JMX Exporter Logs
Inspect JVM standard error output (`stdout`/`stderr` or `catalina.out`) for JMX binding exceptions:
```text
java.net.BindException: Address already in use: 5558
```
Change the exporter port in `kafka.yml` and update `prometheus.yml`.
