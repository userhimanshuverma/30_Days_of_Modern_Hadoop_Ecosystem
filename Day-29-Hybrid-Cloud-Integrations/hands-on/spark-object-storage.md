# Apache Spark Object Storage Hands-On Lab Guide

Step-by-step production lab guide for configuring Apache Spark to read and write multi-cloud object storage (S3, ADLS Gen2, GCS) with S3A Magic Committers and Parquet optimizations.

---

## 1. Submitting Spark Jobs with Cloud Maven Dependencies

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-azure:3.3.4,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.8 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadminpassword \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.committer.name=magic \
  --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.FileOutputCommitterFactory \
  spark_cloud_pipeline.py
```

---

## 2. PySpark Multi-Cloud ETL Script (`spark_cloud_pipeline.py`)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Initialize Spark Session with Cloud Configs
spark = SparkSession.builder \
    .appName("Day29-MultiCloudPipeline") \
    .getOrCreate()

print("1. Reading raw web telemetry logs from S3A...")
s3_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://warehouse/raw_telemetry/*.csv")

# Perform Data Transformations
processed_df = s3_df.filter(col("status_code") == 200) \
    .withColumn("ingestion_time", current_timestamp()) \
    .repartition(4)

print("2. Writing aggregated Parquet dataset back to S3A using Magic Committer...")
processed_df.write \
    .mode("overwrite") \
    .partitionBy("status_code") \
    .parquet("s3a://warehouse/analytics/web_telemetry_parquet")

print("3. Querying ADLS Gen2 Container dataset...")
# Reading from Azure ADLS Gen2
adls_path = "abfs://analytics@stgdatalake.dfs.core.windows.net/user_profiles"
try:
    adls_df = spark.read.parquet(adls_path)
    adls_df.show(5)
except Exception as e:
    print(f"ADLS read skipped or unconfigured: {e}")

print("4. Multi-cloud data processing completed successfully.")
spark.stop()
```

---

## 3. Verifying Zero-Rename Output

Check Spark logs for:
```
INFO committer.AbstractS3ACommitter: S3A committer magic binding to s3a://warehouse/analytics/
INFO cloud.PathOutputCommitProtocol: Using output committer class org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter
```
