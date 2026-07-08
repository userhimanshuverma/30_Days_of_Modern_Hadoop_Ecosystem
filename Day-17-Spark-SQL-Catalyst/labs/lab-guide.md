# Day 17 — Spark SQL & Catalyst Optimizer Hands-On Lab
# Location: Day-17-Spark-SQL-Catalyst/labs/lab-guide.md

## 🎯 Lab Objectives
* Deploy a multi-node Hadoop (HDFS) & Spark Standalone cluster using Docker.
* Submit a customized Spark SQL job showing Catalyst optimizer steps.
* Analyze logical and physical query plans (Parsed, Analyzed, Optimized, Physical).
* Compare execution plans for Broadcast Hash Joins vs. Sort Merge Joins.
* Enable and observe Adaptive Query Execution (AQE) partition coalescing at runtime.
* Run automated validation scripts to check system state.

---

## ⚙️ Environment Provisioning
Instructions to launch the infrastructure:

```bash
# Navigate to the Day 17 docker directory
cd /workspace/Day-17-Spark-SQL-Catalyst/docker

# Launch the Docker Compose stack
docker-compose up -d
```

Verify that all containers are healthy:
```bash
docker-compose ps
```

*Expected Output:*
- `namenode-day17` (Healthy)
- `datanode-day17` (Healthy)
- `spark-master-day17` (Healthy)
- `spark-worker-1-day17` (Healthy)
- `spark-worker-2-day17` (Healthy)
- `spark-history-day17` (Healthy)
- `spark-client-day17` (Up)

---

## 📈 Step-by-Step Lab Tasks

### Task 1: Connect to the Spark Client Container
Launch a bash session inside the client container:
```bash
docker exec -it spark-client-day17 /bin/bash
```
The workspace volume is mounted directly under `/workspace`. All configs, scripts, and source files are available in this container.

---

### Task 2: Execute PySpark Demo Script
Run the custom Spark SQL demo script to generate datasets and output plans:
```bash
python3 /workspace/source/SparkSqlDemo.py
```

This script will:
1. Initialize a `SparkSession` with AQE and Tungsten optimizations enabled.
2. Build two datasets: `users` (100,000 skewed rows) and `cities` (a small reference lookup table).
3. Execute SQL join queries and output plans.
4. Perform Broadcast Hash Joins.
5. Disable Broadcast thresholds to force Sort Merge Joins.
6. Trigger an AQE aggregation stage with 50 initial partitions to observe runtime coalescence.
7. Save query plan exports to `/workspace/source/output/`.

---

### Task 3: Inspect Catalyst Query Plans
Inspect the generated explain file:
```bash
cat /workspace/source/output/explain_plan.txt
```

Verify the following transitions in the output text:
1. **Parsed Logical Plan**: Check if the raw query SQL AST matches the operations.
2. **Analyzed Logical Plan**: Check if the relation columns and data types are resolved using the Catalog database.
3. **Optimized Logical Plan**: Search for `Filter` and `Project` optimizations showing Predicate Pushdown and Column Pruning.
4. **Physical Plan**: Locate scan operators showing physical projection list and pushed filters.

---

### Task 4: Run Automated Verification Scripts
Execute the pre-built validation scripts to programmatically test each component:
```bash
cd /workspace/scripts

# 1. Verify general Spark SQL connectivity and inline queries
./verify-spark-sql.sh

# 2. Verify Catalyst compiler phases (Parsed, Analyzed, Optimized, Physical)
./verify-catalyst.sh

# 3. Verify AQE activation and adaptive optimization nodes
./verify-aqe.sh

# 4. Verify Column Pruning and Predicate Pushdown push down
./verify-query-plan.sh
```

All scripts must return exit code `0` and print success checkmarks (`✅ Success`).

---

### Task 5: Explore the Spark Web UI
On your host browser, access the following dashboards:
* **Spark Master UI (`http://localhost:8080`)**: Check active executor cores and memory.
* **Spark Worker UIs (`http://localhost:8081` / `8082`)**: Inspect stdout/stderr streams.
* **Spark History Server (`http://localhost:18080`)**: Look for completed application `SparkSqlCatalystDemo`. Click into the **SQL / DataFrame** tab to view the physical DAG execution plan. You can visualize the adaptive query execution stages, runtime coalescence nodes, and Whole-Stage Codegen boundary blocks (represented as light-blue shaded boxes).

---

## 🛑 Clean Up
Once finished, exit the client container and tear down the infrastructure:
```bash
exit
cd /workspace/Day-17-Spark-SQL-Catalyst/docker
docker-compose down -v
```
The `-v` flag deletes all HDFS metadata and data blocks, resetting the storage space.
