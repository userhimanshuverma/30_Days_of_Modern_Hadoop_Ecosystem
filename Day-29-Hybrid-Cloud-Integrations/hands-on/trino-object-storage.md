# Trino Object Storage Hands-On Lab Guide

Step-by-step production lab guide for configuring Trino MPP SQL Engine to query multi-cloud object storage datasets directly via Hive Metastore.

---

## 1. Trino Catalog Setup

Create `/etc/trino/catalog/cloud_hive.properties`:

```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083

# S3 / MinIO Configuration
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadminpassword
hive.s3.ssl.enabled=false

# Performance tuning
hive.s3.max-connections=500
hive.non-managed-table-writes-enabled=true
```

---

## 2. Trino CLI Interactive Analytics

Connect using Trino CLI:

```bash
trino --server http://localhost:8081 --catalog cloud_hive --schema default
```

Execute federation queries joining S3 and ADLS tables:

```sql
-- 1. Show Schemas
SHOW SCHEMAS;

-- 2. Query S3 Clickstream Table
SELECT region, COUNT(DISTINCT user_id) as active_users
FROM cloud_hive.cloud_analytics_db.clickstream_s3
GROUP BY region;

-- 3. Cross-Cloud Federation (Join S3 Clickstream with Azure ADLS User Profiles)
SELECT 
    c.session_id,
    c.url,
    u.email,
    u.country
FROM cloud_hive.cloud_analytics_db.clickstream_s3 c
JOIN cloud_hive.cloud_analytics_db.user_profiles_adls u
  ON c.user_id = u.user_id
WHERE c.year = '2026'
LIMIT 100;
```

---

## 3. Explaining Query Execution Plan on Cloud Data

```sql
EXPLAIN ANALYZE
SELECT region, COUNT(*) 
FROM cloud_hive.cloud_analytics_db.clickstream_s3 
GROUP BY region;
```
