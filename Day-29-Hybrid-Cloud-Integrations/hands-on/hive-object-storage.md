# Hive Object Storage Hands-On Lab Guide

Step-by-step production lab guide for creating Hive External Tables, Partition Discovery, and MSCK REPAIR on S3, ADLS Gen2, and GCS.

---

## 1. Hive DDL for External Tables on Object Storage

Connect to Hive Beeline CLI:

```sql
-- 1. Create Hive Database pointing to S3 Bucket Warehouse Location
CREATE DATABASE IF NOT EXISTS cloud_analytics_db
LOCATION 's3a://warehouse/cloud_analytics_db.db';

USE cloud_analytics_db;

-- 2. Create External Table stored as Parquet on S3A
CREATE EXTERNAL TABLE IF NOT EXISTS clickstream_s3 (
    session_id STRING,
    user_id STRING,
    url STRING,
    event_timestamp TIMESTAMP
)
PARTITIONED BY (region STRING, year STRING)
STORED AS PARQUET
LOCATION 's3a://warehouse/cloud_analytics_db.db/clickstream_s3';

-- 3. Create External Table on Azure ADLS Gen2
CREATE EXTERNAL TABLE IF NOT EXISTS user_profiles_adls (
    user_id STRING,
    email STRING,
    country STRING,
    signup_date DATE
)
STORED AS ORC
LOCATION 'abfs://analytics@stgdatalake.dfs.core.windows.net/user_profiles';

-- 4. Create External Table on Google Cloud Storage
CREATE EXTERNAL TABLE IF NOT EXISTS financial_transactions_gcs (
    txn_id STRING,
    amount DOUBLE,
    currency STRING,
    txn_timestamp TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'gs://hadoop-hybrid-lake/financial_transactions';
```

---

## 2. Partition Discovery & Repair

When new object files are written by Spark or external applications under object storage prefixes (e.g. `s3a://warehouse/.../region=us-east/year=2026/`), run:

```sql
-- Discover new partitions on Cloud Storage
MSCK REPAIR TABLE clickstream_s3;

-- Verify Partition Metadata
SHOW PARTITIONS clickstream_s3;
```

---

## 3. Querying Cloud External Tables

```sql
SELECT region, year, COUNT(session_id) as total_sessions
FROM clickstream_s3
WHERE year = '2026'
GROUP BY region, year;
```
