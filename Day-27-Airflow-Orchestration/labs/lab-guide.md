# Day 27 Hands-On Lab: Deploying Apache Airflow and Orchestrating ETL Pipelines

Welcome to the hands-on laboratory for **Day 27: Airflow Orchestration**. In this lab, you will deploy a multi-container Apache Airflow cluster, configure connections, deploy DAGs, run manual backfills, and verify pipeline execution metrics.

---

## 🛠️ Prerequisites

Ensure you have the following packages installed on your local cluster/host:
* **Docker Desktop** (version 20.10.0 or higher)
* **Docker Compose** (version 1.29.0 or higher)
* **Python** (version 3.9+ for running validation scripts locally)
* **curl** (for API validation tests)

---

## 🚀 Phase 1: Deploying the Container Stack

The container network includes PostgreSQL (metadata storage), Redis (message broker), Airflow Web Server, Airflow Scheduler, Celery Workers, and Triggerer.

### Step 1: Initialize the databases and configurations
First, navigate to the docker folder and build/initialize the services:
```bash
cd docker
# Create metadata schemas and default admin user credentials
docker compose run --rm airflow-init
```
*Expected Output:*
```
DB migrations completed.
Admin user 'admin' created successfully with role 'Admin'.
```

### Step 2: Launch the cluster in the background
Start the remaining core components:
```bash
docker compose up -d
```
Check task states to verify health indicators:
```bash
docker compose ps
```
*Expected Output:*
```
NAME                IMAGE               COMMAND                  SERVICE             STATUS              PORTS
docker-postgres-1   postgres:13         "docker-entrypoint.s…"   postgres            running (healthy)   0.0.0.0:5432->5432/tcp
docker-redis-1      redis:6.2-alpine    "docker-entrypoint.s…"   redis               running (healthy)   0.0.0.0:6379->6379/tcp
docker-scheduler-1  docker-airflow      "/usr/bin/entrypoint…"   scheduler           running (healthy)   
docker-webserver-1  docker-airflow      "/usr/bin/entrypoint…"   webserver           running (healthy)   0.0.0.0:8080->8080/tcp
docker-worker-1     docker-airflow      "/usr/bin/entrypoint…"   worker              running             0.0.0.0:8793->8793/tcp
docker-triggerer-1  docker-airflow      "/usr/bin/entrypoint…"   triggerer           running             
```

---

## 📬 Phase 2: Configuring Airflow Connections

In order for the tasks to communicate with external resources (such as Hive or Spark catalogs), you need to register their connections in the Airflow metadata store.

### Step 1: Open the Airflow Web Console
1. Navigate to: `http://localhost:8080` in your web browser.
2. Enter the default administrator credentials:
   * **Username**: `admin`
   * **Password**: `admin_password`

### Step 2: Create a Resource Pool
We need to configure a resource pool named `spark_pool` to limit concurrent Spark tasks to prevent memory issues:
1. Go to **Admin** ➔ **Pools** in the navbar.
2. Click **Create** (+).
3. Set fields:
   * **Pool**: `spark_pool`
   * **Slots**: `2`
   * **Description**: "Limits concurrent Spark job submit threads to avoid YARN thrashing."
4. Click **Save**.

### Step 3: Register SQL Connection
1. Go to **Admin** ➔ **Connections**.
2. Click **Create** (+).
3. Configure the connection fields:
   * **Connection Id**: `hive_metastore_default`
   * **Connection Type**: `Hive Metastore`
   * **Host**: `hive-server`
   * **Port**: `9083`
4. Click **Save**.

---

## 🏃 Phase 3: Triggering and Monitoring Workflows

Let's trigger the ETL DAG we developed (`day_27_hands_on_etl.py`).

### Step 1: Enable the DAG
1. In the Web UI, locate the DAG named `day_27_hands_on_etl`.
2. Toggle the switch on the far left of the row from **Off** to **On**. This registers the DAG in the scheduler cycle.

### Step 2: Trigger the Workflow Manually
1. Click on the DAG ID `day_27_hands_on_etl`.
2. On the top right, click the **Trigger DAG** play button.
3. Select **Trigger DAG w/ config** if you want to override default variables, or simply select **Trigger DAG**.

### Step 3: Inspect Logs and State Progress
1. Select the **Grid** or **Graph** view to monitor task execution statuses.
2. Task boxes turn:
   * **Light Green**: Running
   * **Dark Green**: Succeeded
   * **Red**: Failed (will trigger retry attempts)
3. Click on the task `spark_transform_and_clean` ➔ Click **Log** on the right details pane to inspect the output traces.

---

## 🔄 Phase 4: Running Backfills via CLI

Backfilling is the process of executing a DAG for a historical date range. This is useful when you need to load historical data or reprocess data after fixing a bug.

Execute the following commands inside the running scheduler container:
```bash
# Exec into the scheduler container
docker compose exec scheduler bash

# Run the backfill command for a specific range of days
airflow dags backfill \
  --start-date 2026-07-01 \
  --end-date 2026-07-03 \
  day_27_hands_on_etl
```

*Expected Terminal Log Output:*
```
[2026-07-19 19:35:10] INFO - BackfillJob running. Processing 3 DagRuns.
[2026-07-19 19:35:12] INFO - Creating DagRun day_27_hands_on_etl for execution date 2026-07-01T00:00:00+00:00
[2026-07-19 19:35:15] INFO - TaskInstance finished: generate_transaction_data (success)
...
[2026-07-19 19:35:45] INFO - Backfill run completed. All tasks succeeded.
```

---

## 🧹 Phase 5: Cleanup

When you are done with the labs, clean up container resources to free host memory:
```bash
docker compose down -v
```
*Note:* The `-v` flag deletes named docker volumes (deleting PostgreSQL and Redis states). Omit this if you wish to preserve databases for later labs.
