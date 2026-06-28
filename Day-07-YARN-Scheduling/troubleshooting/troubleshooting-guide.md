# YARN Scheduling Production Troubleshooting Playbook

This playbook provides diagnostic commands, symptoms, root causes, and resolutions for typical resource management and queue scheduling failures in enterprise Hadoop clusters.

---

## 🚨 Issue 1 — Applications Stuck in `ACCEPTED` State

### Symptoms
Jobs are submitted but remain indefinitely in the `ACCEPTED` state, never reaching `RUNNING`. No containers are allocated.

### Root Cause
1. **ApplicationMaster Limits Exceeded**: The queue has reached its maximum AM resource limit (`yarn.scheduler.capacity.maximum-am-resource-percent`). If a queue is saturated with many running ApplicationMasters (each consuming 1 container), YARN prevents new jobs from launching their AMs to avoid deadlocks.
2. **Resource Contention**: The overall cluster memory or CPU is fully exhausted.
3. **Queue Saturated**: The queue has reached its absolute maximum capacity.

### Diagnostic Commands
Check the active applications in the queue:
```bash
yarn application -list
```
Get queue properties and active AM resource usage:
```bash
yarn queue -status root.prod.finance
```
Look for scheduler logs in ResourceManager:
```bash
docker logs resourcemanager-day07 2>&1 | grep -i "am-resource"
```

### Resolution
1. **Increase AM limit**: In `capacity-scheduler.xml`, increase the AM threshold:
   ```xml
   <property>
     <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
     <value>0.35</value> <!-- Increase from default 0.10/0.20 -->
   </property>
   ```
2. **Kill idle applications**: Stop lower-priority or stuck applications:
   ```bash
   yarn application -kill <application_id>
   ```
3. **Raise Queue Max Capacity**: Increase the maximum capacity constraint for the queue.

---

## 🚨 Issue 2 — Queue Starvation (Low-Priority Queue Dominance)

### Symptoms
High-priority production jobs are pending, while sandbox/dev tasks continue to execute, hogging all resources.

### Root Cause
1. **Preemption is Disabled**: Preemption is turned off by default. High-priority queues cannot reclaim resources from over-allocated lower-priority queues.
2. **Resource Calculator configuration**: Using memory-only calculation (`DefaultResourceCalculator`) when jobs are CPU-bound, causing vCore saturation to go unnoticed by YARN.

### Diagnostic Commands
Verify if preemption is enabled:
```bash
hdfs getconf -confKey yarn.resourcemanager.scheduler.monitor.enable
```
Verify the Resource Calculator:
```bash
hdfs getconf -confKey yarn.scheduler.capacity.resource-calculator
```

### Resolution
1. **Enable Preemption**: In `yarn-site.xml`, enable the preemption monitor:
   ```xml
   <property>
     <name>yarn.resourcemanager.scheduler.monitor.enable</name>
     <value>true</value>
   </property>
   ```
2. **Enable Dominant Resource Calculator (DRC)**: Use DRC to evaluate both Memory and CPU:
   ```xml
   <property>
     <name>yarn.scheduler.capacity.resource-calculator</name>
     <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
   </property>
   ```

---

## 🚨 Issue 3 — User Limit Factor Constraints

### Symptoms
A queue has plenty of available capacity, but applications submitted by a specific user remain stuck in `ACCEPTED` or pending container status.

### Root Cause
The queue's user limit factor (`yarn.scheduler.capacity.root.<queue_path>.user-limit-factor`) restricts a single user from consuming more than a configured share. If set to `1`, a single user cannot use more than the queue's *guaranteed minimum* capacity, even if the queue is 100% idle.

### Diagnostic Commands
Check user limit factor configuration:
```bash
hdfs getconf -confKey yarn.scheduler.capacity.root.prod.finance.user-limit-factor
```

### Resolution
Increase the user limit factor (e.g., to `3` or `4`) to allow a single user to elasticize and take over multiple times the queue's guaranteed minimum when other users are inactive:
```xml
<property>
  <name>yarn.scheduler.capacity.root.prod.finance.user-limit-factor</name>
  <value>3</value>
</property>
```

---

## 🚨 Issue 4 — Resource Fragmentation

### Symptoms
The cluster displays available memory (e.g., 2GB free) and available vCores (e.g., 2 vCores free), but containers are not being scheduled.

### Root Cause
The free resources are scattered across different NodeManagers. A container request of 2GB cannot be scheduled if NodeManager 1 has 1GB free and NodeManager 2 has 1GB free.

### Diagnostic Commands
Print node details:
```bash
yarn node -list -all
```

### Resolution
1. Align container request sizes with node physical increments.
2. Reduce the minimum allocation sizing (`yarn.scheduler.minimum-allocation-mb`).
3. Leverage Node Labels or partition nodes to direct large memory tasks to dedicated high-memory hosts.
