# Hands-On Lab: Configuring Multiple Queues and Demonstrating Multi-Tenant Scheduling

This lab walks you through establishing, monitoring, and evaluating a multi-tenant queue system in a YARN cluster. We will explore queue configurations, submit parallel workloads representing different teams, and observe resource preemption in action.

---

## 🏗️ Topology Overview
Our cluster has:
* **ResourceManager (`resourcemanager-day07`)**: The cluster coordinator.
* **NodeManager 1 (`nodemanager1-day07`)**: Worker node (4GB RAM, 4 vCores).
* **NodeManager 2 (`nodemanager2-day07`)**: Worker node (4GB RAM, 4 vCores).
* **NameNode & DataNode**: Storage backplane.
* **HistoryServer**: MapReduce job statistics registry.

We use **Capacity Scheduler** with three main queues:
* `root.default` (20% capacity)
* `root.prod` (60% capacity) with subqueues `finance` (24% of cluster) and `marketing` (36% of cluster)
* `root.dev` (20% capacity) with subqueue `datascience` (20% of cluster)

---

## 🛠️ Step 1 — Startup the Cluster
Navigate to the `docker/` folder and launch the services:

```bash
cd docker
docker compose up -d
```

### Expected Output
```text
Creating network "day07-network" with driver "bridge"
Creating volume "docker_namenode_data_day07" with default driver
Creating volume "docker_datanode_data_day07" with default driver
Creating volume "docker_historyserver_data_day07" with default driver
Creating namenode-day07 ... done
Creating datanode-day07 ... done
Creating resourcemanager-day07 ... done
Creating nodemanager1-day07    ... done
Creating nodemanager2-day07    ... done
Creating historyserver-day07   ... done
```

Wait 15–20 seconds for the Java processes to boot and negotiate HDFS connections.

---

## 🔍 Step 2 — Verify Cluster Registrations
Run the resource validation script from the repository root:

```bash
./scripts/verify-resource-allocation.sh
```

### Expected Output
```text
=== YARN Cluster Node Resource Allocations ===
Total Registered NodeManagers: 2

Node ID: nodemanager1-day07:8042
  Hostname:  nodemanager1-day07
  State:     RUNNING
  Containers Running: 0
  Memory allocation:  0 MB / 4096 MB (0.0% used)
  CPU Core allocation: 0 vCores / 4 vCores (0.0% used)
--------------------------------------------------
Node ID: nodemanager2-day07:8042
  Hostname:  nodemanager2-day07
  State:     RUNNING
  Containers Running: 0
  Memory allocation:  0 MB / 4096 MB (0.0% used)
  CPU Core allocation: 0 vCores / 4 vCores (0.0% used)
--------------------------------------------------
```

---

## 📈 Step 3 — Verify YARN Queue Hierarchies
Run the queue hierarchy verification script:

```bash
./scripts/verify-queues.sh
```

### Expected Output
```text
=== YARN Queue Hierarchy and Allocation Analyzer ===
Active YARN Scheduler Queues (REST Data Analyzer):

Scheduler Type: capacityScheduler
├── Queue: default         | Configured Cap:  20.0% | Max:  50.0% | Used:   0.0% | State: RUNNING
├── Queue: prod            | Configured Cap:  60.0% | Max: 100.0% | Used:   0.0% | State: RUNNING
    ├── Queue: finance         | Configured Cap:  40.0% | Max:  80.0% | Used:   0.0% | State: RUNNING
    ├── Queue: marketing       | Configured Cap:  60.0% | Max: 100.0% | Used:   0.0% | State: RUNNING
├── Queue: dev             | Configured Cap:  20.0% | Max:  40.0% | Used:   0.0% | State: RUNNING
    ├── Queue: datascience     | Configured Cap: 100.0% | Max: 100.0% | Used:   0.0% | State: RUNNING
```

---

## 🚀 Step 4 — Run Multi-Tenant Simulation
Submit four concurrent MapReduce jobs to the respective queues:

```bash
./scripts/submit-multi-tenant-demo.sh
```

This starts background Pi estimation runs in `root.prod.finance`, `root.prod.marketing`, `root.dev.datascience`, and `root.default`.

### Expected Output
```text
Submitting Concurrent Jobs to Multiple Queues...
🚀 [Job 1] Submitting Finance MR Pi Job to root.prod.finance (Capacity: 24%)
🚀 [Job 2] Submitting Marketing MR Pi Job to root.prod.marketing (Capacity: 36%)
🚀 [Job 3] Submitting DataScience MR Pi Job to root.dev.datascience (Capacity: 20%)
🚀 [Job 4] Submitting Sandbox default MR Pi Job to root.default (Capacity: 20%)

--- Application Status Scan (1/5) ---
Total Applications: 4
Application-Id       Application-Name   Application-Type  User      Queue               State      Final-State
application_1234_001  PiEstimator        MAPREDUCE         root      root.prod.finance   RUNNING    UNDEFINED
application_1234_002  PiEstimator        MAPREDUCE         root      root.prod.marketing RUNNING    UNDEFINED
application_1234_003  PiEstimator        MAPREDUCE         root      root.dev.datascience RUNNING    UNDEFINED
application_1234_004  PiEstimator        MAPREDUCE         root      root.default        RUNNING    UNDEFINED
```

---

## ⚡ Step 5 — Observe Resource Preemption
Let's see YARN preemption in action:
1. Submit a heavy job to `root.default` with a high container count request, filling up all 8GB of cluster memory.
2. While that job is running, submit a new job to the high-priority `root.prod.finance` queue.
3. Because preemption is enabled (`yarn.resourcemanager.scheduler.monitor.enable=true`), the ResourceManager will detect that `root.prod.finance` is starving (it is below its guaranteed 24% capacity) and that `root.default` is using more than its configuration capacity (20%).
4. The ResourceManager will issue preemption warnings, wait for 15 seconds, and then kill containers assigned to `root.default` to free memory.
5. Watch the ResourceManager logs:

```bash
docker logs resourcemanager-day07 2>&1 | grep -i "preempt"
```

### Expected Preemption Log Output
```text
INFO monitor.CapacitySchedulerPreemptionMonitor: System resources are under-allocated. Scanning for containers to preempt...
INFO monitor.ProportionalCapacityPreemptionPolicy: Queue root.default is over capacity. Candidates for preemption identified.
INFO monitor.ProportionalCapacityPreemptionPolicy: Preempting container_17195822394_0004_01_000003 from application_17195822394_0004 to reclaim resources for queue root.prod.finance.
WARN resourcemanager.RMNodeImpl: Container container_17195822394_0004_01_000003 completed with exit code -105 (Container preempted by scheduler).
```

---

## 🧹 Step 6 — Cleanup
Stop and remove all Docker containers and volumes:

```bash
cd docker
docker compose down -v
```
