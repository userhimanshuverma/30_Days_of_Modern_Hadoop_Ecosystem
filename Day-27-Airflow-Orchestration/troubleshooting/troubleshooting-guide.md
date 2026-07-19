# Day 27 Operations Playbook: Troubleshooting Apache Airflow in Production

This guide contains diagnostic steps, root cause analyses, and mitigation playbooks for common operational failures encountered when running Apache Airflow clusters in high-throughput enterprise environments.

---

## 🗺️ Rapid Diagnostics Checklist

| Symptoms | Likely Culprit | Log to Inspect | Quick Resolution |
| :--- | :--- | :--- | :--- |
| **New DAG file not visible in Web UI** | Serialization or syntax errors. | `dag-processor.log` | Run `airflow dags reserialize` or compile locally. |
| **Tasks stuck indefinitely in `queued` state** | Insufficient slots in pool, workers dead, or Celery DB/Broker down. | `scheduler.log`, worker status. | Check pool allocations, verify Celery worker daemon. |
| **Task logs fail to load in Web UI** | Port 8793 unreachable or remote logging write failed. | `webserver.log`, worker log. | Check security groups/ports, ensure S3/HDFS write permissions. |
| **Metadata DB connection pool errors** | Exhausted db limits. | `postgres.log`, `airflow.cfg` | Tune `sql_alchemy_pool_size` and check active connections. |
| **Scheduler stops processing runs** | CPU saturation, database lock contention, deadlocks. | `scheduler.log` | Implement HA Scheduler (active-active) and increase db memory. |

---

## 🛠️ Incident Recovery Playbooks

### Playbook 1: Diagnosing "DAG Not Appearing in the UI"

#### Symptoms:
An engineer drops a new file `/opt/airflow/dags/my_pipeline.py` into the DAGs folder, but it does not appear in the Web UI dashboard.

#### Root Causes:
1. **Python Syntax Error**: The scheduler's DAG parser process failed to compile the file.
2. **Missing DAG Definition**: The file does not define a global object of class `airflow.models.dag.DAG`.
3. **DAG Serialization Inconsistency**: The webserver cannot synchronize serialized representations from the metadata DB.

#### Resolution Steps:
1. Run syntax and import validation tests inside the scheduler container:
   ```bash
   airflow dags list-import-errors
   ```
2. Manually test parsing the specific file to check for compilation stack traces:
   ```bash
   python /opt/airflow/dags/my_pipeline.py
   ```
3. Force a manual sync of serialized DAG definitions to the database:
   ```bash
   airflow dags reserialize
   ```

---

### Playbook 2: Resolving "Tasks Stuck in Queued State"

#### Symptoms:
Task boxes display a gray outline (`queued`) for hours, never transitioning to a light green (`running`) state.

#### Root Causes:
1. **Resource Pool Saturation**: The task is assigned to a pool (e.g., `spark_pool`) whose active slots are completely full.
2. **Executor Queue Lag**: In a Celery Executor configuration, the Celery queue (e.g., Redis) is backlog-saturated, or the Celery worker daemon has stopped polling.
3. **Concurrency Limits Reached**: The DAG has hit its `max_active_runs` or `dag_concurrency` configuration limits.

#### Resolution Steps:
1. Query active pool usage statistics via the CLI:
   ```bash
   airflow pools list
   ```
2. Check if the task is blocked by DAG/Scheduler concurrency limits:
   ```bash
   airflow jobs check --job-type SchedulerJob
   ```
3. Check status of Celery workers and broker connectivity:
   ```bash
   # Check worker status
   celery -A airflow.providers.celery.executors.celery_executor control ping
   # Inspect Redis backlog size
   redis-cli -a redis_secure_pass llen default
   ```
4. If workers are dead, restart the service:
   ```bash
   docker compose restart worker
   ```

---

### Playbook 3: Debugging "Missing Task Logs"

#### Symptoms:
Clicking on a task instance in the Web UI outputs the error: `Could not read log from worker. Connection refused (8793)` or similar.

#### Root Causes:
1. **Worker Network Port Blocked**: The Webserver accesses task logs from an HTTP server running on Celery workers (default port `8793`). Firewall rules or container namespaces block this port.
2. **Worker Ephemeral State**: Celery workers write logs to local disk storage. If a worker pod restarts, local logs are wiped.
3. **Storage Write Failure**: In remote logging setups, the worker lacked IAM permissions to write logs to S3, GCS, or HDFS.

#### Resolution Steps:
1. Verify if port `8793` is open and reachable from the webserver container:
   ```bash
   curl -I http://worker:8793/log/day_27_hands_on_etl/generate_transaction_data/2026-07-01T00:00:00/1.log
   ```
2. Configure **Remote Logging** in `airflow.cfg` to persist log files to central object storage. This ensures logs are visible even if workers are killed:
   ```ini
   [logging]
   remote_logging = True
   remote_base_log_folder = s3://company-airflow-logs/prod/
   remote_log_conn_id = aws_s3_log_conn
   ```
3. Audit worker container log output to ensure no write-permission failures occur:
   ```bash
   docker compose logs worker | grep -E "Error|Exception"
   ```

---

### Playbook 4: Mitigating Metadata Database Deadlocks

#### Symptoms:
Scheduler logs print SQL connection errors: `sqlalchemy.exc.OperationalError: (psycopg2.errors.DeadlockDetected)` or connection timeout warnings.

#### Root Causes:
1. **Too Many Scheduler Threads**: `max_threads` in `airflow.cfg` is set too high, overloading PostgreSQL's transaction manager.
2. **Unindexed Custom Metadata Queries**: Custom plugins or hooks are performing heavy unindexed table scans on `task_instance` or `dag_run` tables.
3. **Under-provisioned DB Resources**: The DB CPU or IOPS limits are saturated.

#### Resolution Steps:
1. Tune database connection pool sizes down in `airflow.cfg`:
   ```ini
   [database]
   sql_alchemy_pool_size = 10
   sql_alchemy_max_overflow = 5
   ```
2. Check database engine active session counts and lock contention (PostgreSQL query):
   ```sql
   SELECT pid, age(clock_timestamp(), query_start), usename, query, state 
   FROM pg_stat_activity 
   WHERE state != 'idle' AND age(clock_timestamp(), query_start) > interval '5 minutes';
   ```
3. Run index optimization maintenance on the Airflow database:
   ```bash
   airflow db clean --clean-before-date '2026-06-01'
   ```
