# Troubleshooting Playbook: Alertmanager Routing & Notification Failure

## Symptoms
- Prometheus UI shows alert in state `FIRING`, but no notification is received in Slack or PagerDuty.
- Duplicate alerts flooding Slack channels.

## Root Causes
1. **Webhook / API Auth Failure**: Invalid Slack Webhook URL or expired PagerDuty API integration key.
2. **Inhibition Over-Match**: An inhibition rule matched prematurely, silencing warning/critical alerts.
3. **Mismatched Group Labels**: `group_by` array in `alertmanager.yml` contains labels missing from the firing alert.

## Diagnostic Steps

### 1. Test Alertmanager Config Syntax
Validate configuration using `amtool`:
```bash
amtool check-config /etc/alertmanager/alertmanager.yml
```

### 2. Trace Alert Routing
Use `amtool` to simulate route matching:
```bash
amtool config routes show --alertmanager.url=http://localhost:9093
```

### 3. Check Active Silences
List active silences in Alertmanager:
```bash
amtool silence query --alertmanager.url=http://localhost:9093
```
