# dags/case_study_dag.py
"""
Case Study: Retail Bank Transaction Ingestion & Analytics Pipeline
Orchestrates: Applications -> Kafka -> Spark Core ETL -> Hive Server -> Trino OLAP -> BI Dashboard.

Highlights:
1. Conditional branching based on system metrics (BranchPythonOperator)
2. Handling tasks with various Trigger Rules (all_success, one_failed)
3. Custom SLA configurations and Slack alert callback simulation
4. Task dependencies with multiple parallel streams merging
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------
# PARAMETERS & CALLBACKS
# ----------------------------------------------------
default_args = {
    'owner': 'retail_bank_data',
    'depends_on_past': True, # Enforces sequential data consistency
    'start_date': datetime(2026, 7, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=45),
}

def slack_alert_failure(context):
    """
    Sends detailed error payloads to a Slack channel webhook.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    err = context.get('exception')
    logging.info(f"🚨 [Slack Alert Webhook Triggered]: Task '{task_id}' in DAG '{dag_id}' failed with exception: {err}")

def audit_branch_selection(**kwargs):
    """
    Checks the record ingestion volume in Kafka. 
    If data size is high, branches to the large clusters Spark cluster; 
    otherwise, routes to the standard cluster Spark job.
    """
    # Simulate scanning a metric system or Kafka offset counts
    logical_date = kwargs.get('ds')
    logging.info(f"Analyzing transaction volume offsets for logical date: {logical_date}")
    
    # In a real environment, we'd pull offsets from Spark logs or Kafka REST proxy.
    record_count = 1200000 # Simulated count
    
    if record_count > 1000000:
        logging.info("Volume exceeds threshold (1M). Branching to 'spark_yarn_large_cluster'.")
        return 'spark_yarn_large_cluster'
    else:
        logging.info("Volume is within normal range. Branching to 'spark_yarn_standard_cluster'.")
        return 'spark_yarn_standard_cluster'

# ----------------------------------------------------
# DAG WORKFLOW
# ----------------------------------------------------
with DAG(
    dag_id='bank_transaction_orchestrator',
    default_args=default_args,
    description='Enterprise bank transaction ETL, Trino caching, and dashboard syncer',
    schedule_interval='0 6 * * *', # Runs every day at 06:00 AM UTC
    catchup=True,                  # Backfill historic dates to initialize warehouse
    max_active_runs=3,
    on_failure_callback=slack_alert_failure,
    tags=['banking', 'security', 'trino', 'spark', 'ha'],
) as dag:

    # Step 1: Initialize workflow execution state
    start_pipeline = EmptyOperator(
        task_id='start_ingestion_pipeline'
    )

    # Step 2: Validate Kafka topic cluster connection and schema compatibility
    verify_kafka_health = BashOperator(
        task_id='verify_kafka_health',
        bash_command="kafka-topics.sh --bootstrap-server kafka:9092 --list | grep transaction-logs",
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    # Step 3: Run Branching logic to decide size of execution resource pool
    determine_scale = BranchPythonOperator(
        task_id='determine_spark_scaling',
        python_callable=audit_branch_selection,
        provide_context=True,
    )

    # Step 4a: Large capacity Spark submission on YARN (high memory/cores allocation)
    spark_large = BashOperator(
        task_id='spark_yarn_large_cluster',
        bash_command=(
            "spark-submit --master yarn --deploy-mode cluster "
            "--num-executors 16 --executor-cores 4 --executor-memory 16G "
            "/opt/airflow/scripts/spark_jobs/bank-ingest.jar "
            "--dt {{ ds }} --scale large"
        ),
    )

    # Step 4b: Standard capacity Spark submission on YARN
    spark_standard = BashOperator(
        task_id='spark_yarn_standard_cluster',
        bash_command=(
            "spark-submit --master yarn --deploy-mode cluster "
            "--num-executors 4 --executor-cores 2 --executor-memory 4G "
            "/opt/airflow/scripts/spark_jobs/bank-ingest.jar "
            "--dt {{ ds }} --scale standard"
        ),
    )

    # Step 5: Join branch paths using TriggerRule 'one_success' (or 'none_failed')
    merge_transformations = EmptyOperator(
        task_id='merge_transformations',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Step 6: Load results into Hive Warehouse tables and audit tables
    load_hive_table = BashOperator(
        task_id='load_hive_table',
        bash_command="hive -f /opt/airflow/scripts/sql/load_bank_transactions.sql -d dt={{ ds }}",
    )

    # Step 7: Warm Cache in Trino Engine for low-latency BI dashboards queries
    warm_trino_cache = BashOperator(
        task_id='warm_trino_cache',
        bash_command="trino --server trino-coordinator:8080 --execute \"CALL system.runtime.warm_cache('warehouse', 'transactions');\"",
    )

    # Step 8: Sync Dashboard parameters
    trigger_dashboard_sync = BashOperator(
        task_id='trigger_dashboard_sync',
        bash_command="curl -X POST http://superset:8088/api/v1/dashboard/transactions/refresh",
    )

    # Step 9: Alert system administrators if spark processes fail
    alert_recovery = BashOperator(
        task_id='trigger_critical_oncall_alert',
        bash_command="echo 'Sending critical bank pipeline alert. Spark failed!'",
        trigger_rule=TriggerRule.ONE_FAILED, # Triggers ONLY if an upstream task fails
    )

    # Step 10: Complete pipeline successfully
    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ----------------------------------------------------
    # TASK DEPENDENCY GRAPH
    # ----------------------------------------------------
    # Branching structure:
    # start -> kafka_health -> branch -> [large / standard] -> merge -> hive -> trino -> dashboard -> end
    start_pipeline >> verify_kafka_health >> determine_scale
    determine_scale >> [spark_large, spark_standard] >> merge_transformations
    
    # Error alerting stream:
    # If spark large or standard fails, trigger alert_recovery. Otherwise bypass it.
    [spark_large, spark_standard] >> alert_recovery
    
    merge_transformations >> load_hive_table >> warm_trino_cache >> trigger_dashboard_sync >> end_pipeline
    alert_recovery >> end_pipeline
