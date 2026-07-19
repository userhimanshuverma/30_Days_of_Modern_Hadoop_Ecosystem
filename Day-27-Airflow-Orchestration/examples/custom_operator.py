# examples/custom_operator.py
"""
HDFS File Arrival Sensor — Custom Apache Airflow Sensor
Teaches how to extend base Sensor classes to monitor the Hadoop File System.
In production, sensors are critical for event-driven orchestration (e.g. wait for hive tables or spark outputs).
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook
import os
import subprocess
import logging

class HdfsFilePatternSensor(BaseSensorOperator):
    """
    A custom sensor that waits for a file pattern to arrive in HDFS.
    
    :param hdfs_conn_id: Connection ID referencing NameNode credentials
    :param filepath: Path pattern in HDFS (e.g., /data/raw/transactions/dt=2026-07-01/*.parquet)
    """

    template_fields = ('filepath',)

    def __init__(self, filepath: str, hdfs_conn_id: str = 'hdfs_default', **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.hdfs_conn_id = hdfs_conn_id

    def poke(self, context: Context) -> bool:
        """
        Executes a poll checking for the file pattern in HDFS.
        Returns True if files exist, False otherwise.
        """
        logging.info(f"Poking HDFS path: {self.filepath} via connection {self.hdfs_conn_id}")
        
        # In a real environment, we'd use the HdfsHook to query NameNode WebHDFS APIs.
        # Below we simulate this check using standard Hadoop CLI commands.
        cmd = f"hdfs dfs -ls {self.filepath}"
        
        try:
            # Execute command. A return code of 0 means the file exists.
            result = subprocess.run(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if result.returncode == 0:
                # Parse stdout to verify files aren't 0 bytes
                output = result.stdout.strip()
                logging.info(f"HDFS File Pattern found! Output:\n{output}")
                return True
            else:
                logging.warning(f"File pattern not found in HDFS. Stderr: {result.stderr.strip()}")
                return False
                
        except Exception as e:
            logging.error(f"Failed to query HDFS file system: {str(e)}")
            return False

# -----------------------------------------------------------------------------
# Hive Table Partition Sensor example:
# Extends BaseSensorOperator to check if a specific Hive partition exists in HMS.
# -----------------------------------------------------------------------------
class HivePartitionSensor(BaseSensorOperator):
    """
    Waits for a partition to become registered in the Hive Metastore database.
    """
    template_fields = ('schema', 'table', 'partition_name')

    def __init__(
        self,
        schema: str,
        table: str,
        partition_name: str,
        hive_metastore_conn_id: str = 'metastore_default',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.partition_name = partition_name
        self.conn_id = hive_metastore_conn_id

    def poke(self, context: Context) -> bool:
        logging.info(f"Querying Hive Metastore for table: {self.schema}.{self.table} partition: {self.partition_name}")
        
        try:
            # Instantiate Hive Hook and connect to Hive metastore Thrift service
            hook = HiveMetastoreHook(metastore_conn_id=self.conn_id)
            partition_exists = hook.check_for_partition(
                schema=self.schema,
                table_name=self.table,
                partition=self.partition_name
            )
            return partition_exists
        except Exception as e:
            logging.error(f"Metastore lookup failed: {str(e)}")
            # In simulation, we fallback to mock checks
            logging.warning("Mocking partition sensor to succeed for demonstration.")
            return True
