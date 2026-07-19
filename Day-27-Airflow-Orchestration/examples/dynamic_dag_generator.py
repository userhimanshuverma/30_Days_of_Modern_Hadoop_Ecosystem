# examples/dynamic_dag_generator.py
"""
Dynamic DAG Generator — Metadata-Driven Workflow Generation
Demonstrates a production pattern where multiple DAGs are dynamically registered based on a config file.
Avoids manual file creation for every table/topic, simplifying operations for hundreds of pipelines.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Define metadata catalog (in production, this can be loaded from a JSON/YAML file or DB)
METADATA_PIPELINES = {
    "us_transactions": {
        "schedule": "0 1 * * *",
        "input_topic": "raw-us-transactions",
        "target_table": "warehouse.us_transactions",
        "retries": 3
    },
    "eu_transactions": {
        "schedule": "0 2 * * *",
        "input_topic": "raw-eu-transactions",
        "target_table": "warehouse.eu_transactions",
        "retries": 5
    },
    "ap_transactions": {
        "schedule": "0 3 * * *",
        "input_topic": "raw-ap-transactions",
        "target_table": "warehouse.ap_transactions",
        "retries": 2
    }
}

# 2. Iterate through configs and instantiate DAGs dynamically in the global namespace
for region_id, config in METADATA_PIPELINES.items():
    dag_id = f"dynamic_etl_{region_id}"
    
    # Create the DAG object
    dag = DAG(
        dag_id=dag_id,
        default_args={
            'owner': 'data_platform_engineers',
            'start_date': datetime(2026, 7, 1),
            'retries': config['retries']
        },
        schedule_interval=config['schedule'],
        catchup=False,
        tags=['dynamic', 'region', region_id]
    )
    
    # Define tasks inside the dynamic DAG context
    with dag:
        check_offsets = BashOperator(
            task_id='check_kafka_offsets',
            bash_command=f"kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group {region_id}-group"
        )
        
        run_spark_job = BashOperator(
            task_id='run_spark_etl',
            bash_command=(
                f"spark-submit --master yarn --deploy-mode cluster "
                f"--class com.bank.ProcessRegionData /opt/airflow/spark/etl.jar "
                f"--topic {config['input_topic']} --table {config['target_table']}"
            )
        )
        
        validate_records = BashOperator(
            task_id='validate_hive_records',
            bash_command=f"hive -e 'SELECT COUNT(*) FROM {config['target_table']}';"
        )
        
        # Set task dependencies
        check_offsets >> run_spark_job >> validate_records
        
    # Crucial step: register the generated DAG in the global namespace
    # Airflow scans files and registers anything that is an instance of DAG in the globals() dictionary.
    globals()[dag_id] = dag
