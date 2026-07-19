# dags/hands_on_etl.py
"""
Day 27 Lab: Enterprise ETL Pipeline Orchestration
Demonstrates a production-grade Apache Airflow DAG orchestrating an end-to-end flow:
Generate Data -> Ingest (Kafka) -> Process (Spark) -> Load (Hive) -> Validate -> Notify.

This DAG showcases:
1. Operational best practices (retries, retry_delay, SLA, email alerts)
2. Task relationship definitions (bitshift operators)
3. Dynamic parameters and templates (Jinja2 expressions)
4. Resource management (using pools for Spark submissions)
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator
from airflow.utils.email import send_email

# ----------------------------------------------------
# DEFAULT PIPELINE CONFIGURATIONS
# ----------------------------------------------------
# Define production-grade default arguments
default_args = {
    'owner': 'data_platform_ops',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'email': ['platform-alerts@enterprise.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'sla': timedelta(hours=1), # If execution takes > 1 hour, trigger SLA miss alerts
}

# Callback functions for workflow states
def on_dag_failure(context):
    """
    Triggered when the entire DAG fails. Performs cleanup and alerts Slack/pager duty.
    """
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('logical_date')
    err_msg = context.get('exception')
    logging.error(f"🔴 CRITICAL ERROR: DAG {dag_id} failed for execution run {execution_date}. Error: {err_msg}")
    # Example alert notification logic (simulated)
    # send_email(to='oncall@enterprise.com', subject=f'DAG FAILED: {dag_id}', html_content=str(err_msg))

def on_task_success(context):
    """
    Triggered when critical tasks succeed. Good for auditing.
    """
    task_id = context.get('task_instance').task_id
    logging.info(f"✅ TASK COMPLETED SUCCESSFULLY: {task_id}")

# ----------------------------------------------------
# PYTHON LOGIC SIMULATIONS
# ----------------------------------------------------
def generate_raw_payloads(**kwargs):
    """
    Python task simulating the creation of transaction JSON events.
    Writes metadata into XComs for downstream tasks.
    """
    logical_date = kwargs.get('ds')
    logging.info(f"Generating synthetic financial transaction logs for logical date: {logical_date}")
    
    # Store dynamic information in XCom for downstream validation scripts
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='total_records_generated', value=50000)
    task_instance.xcom_push(key='target_directory', value=f'/data/raw/transactions/{logical_date}')
    
    return "Payload generation complete."

# ----------------------------------------------------
# DAG DEFINITION
# ----------------------------------------------------
with DAG(
    dag_id='day_27_hands_on_etl',
    default_args=default_args,
    description='Production-grade ETL pipeline orchestrating Spark and Hive on Hadoop',
    schedule_interval='@daily', # Runs daily at midnight UTC (0 0 * * *)
    catchup=False,              # Prevent backfilling all historic runs since start_date
    max_active_runs=2,          # Limit concurrent runs to avoid overwhelming database pools
    on_failure_callback=on_dag_failure,
    tags=['production', 'etl', 'spark', 'hive'],
) as dag:

    # Task 1: Generate synthetic transaction datasets
    generate_data = PythonOperator(
        task_id='generate_transaction_data',
        python_callable=generate_raw_payloads,
        provide_context=True,
    )

    # Task 2: Ingest generated raw files to a Kafka Topic using Docker CLI
    # In production, this would invoke a custom KafkaProducerOperator or Kafka Connect job.
    publish_to_kafka = BashOperator(
        task_id='publish_to_kafka',
        bash_command=(
            "echo 'Streaming files from {{ ti.xcom_pull(key=\"target_directory\") }} into Kafka topic raw-transactions...' && "
            "sleep 3 && " # Simulating delay
            "echo 'Published 50000 records successfully.'"
        ),
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    # Task 3: Trigger Apache Spark Streaming/Batch Job to extract raw JSONs and process schema mapping
    # Assign to the 'spark_pool' to limit concurrent Spark tasks in the Airflow cluster.
    spark_transform = BashOperator(
        task_id='spark_transform_and_clean',
        bash_command=(
            "spark-submit --master yarn --deploy-mode cluster "
            "--class com.enterprise.etl.ProcessTransactions "
            "/opt/airflow/scripts/spark_jobs/spark-etl.jar "
            "--input-path {{ ti.xcom_pull(task_ids='generate_transaction_data', key='target_directory') }} "
            "--output-path /data/warehouse/cleaned_transactions/{{ ds }}"
        ),
        pool='spark_pool', # Resource pool limit
        env={'SPARK_HOME': '/opt/spark', 'HADOOP_CONF_DIR': '/etc/hadoop/conf'},
    )

    # Task 4: Load the processed partition into Hive Table metastore
    hive_load_partition = BashOperator(
        task_id='hive_load_partition',
        bash_command=(
            "hive -e \"ALTER TABLE warehouse.cleaned_transactions ADD IF NOT EXISTS PARTITION (dt='{{ ds }}') "
            "LOCATION '/data/warehouse/cleaned_transactions/{{ ds }}';\""
        ),
    )

    # Task 5: Data quality audit - assert that row counts in Hive match generated row counts
    # This acts as a gatekeeper to prevent downstream dashboard corruption.
    verify_record_count = PythonOperator(
        task_id='verify_record_count',
        python_callable=lambda **kwargs: logging.info("Verifying row count matching. Simulating SQL check... Assert count = 50000"),
        on_success_callback=on_task_success,
    )

    # Task 6: Trigger PagerDuty/Email notifications confirming pipeline completion
    send_pipeline_report = BashOperator(
        task_id='send_pipeline_report',
        bash_command="echo 'ETL Pipeline successfully completed for execution date: {{ ds }}. Sending notifications...'",
    )

    # ----------------------------------------------------
    # TASK DEPENDENCY GRAPH
    # ----------------------------------------------------
    # Define execution sequence:
    # Generate -> Kafka -> Spark -> Hive -> Verify -> Notify
    generate_data >> publish_to_kafka >> spark_transform >> hive_load_partition >> verify_record_count >> send_pipeline_report
