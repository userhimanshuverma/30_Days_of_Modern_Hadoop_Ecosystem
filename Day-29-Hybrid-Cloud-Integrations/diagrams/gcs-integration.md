# Google Cloud Storage (GCS) Integration Diagrams

Architecture diagrams for the GoogleHadoopFileSystem (GHFS) GCS connector, service account authentication, and Workload Identity federation.

---

## 1. GCS Connector Engine Architecture

```mermaid
graph TB
    subgraph Hadoop_Workload ["Hadoop Ecosystem Compute"]
        SPARK["Spark Core / SQL"]
        HIVE["Hive Metastore / HiveServer2"]
    end

    subgraph GHFS_Connector ["com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"]
        GHFS_MAIN["GoogleHadoopFileSystem (gs://)"]
        GCS_FS["GoogleHadoopFS Stub"]
        JSON_KEY["Service Account Key Handler"]
        WORKLOAD_ID["GCP Workload Identity Provider"]
        INMEMORY_CACHE["Directory Tree In-Memory Cache"]
    end

    subgraph GCP_Storage_Cloud ["Google Cloud Infrastructure"]
        GCP_IAM["Google Cloud IAM Service"]
        GCS_API["Google Cloud Storage JSON/gRPC API"]
        BUCKET["GCS Storage Bucket"]
    end

    Hadoop_Workload -->|fs.listStatus / fs.open| GHFS_MAIN
    GHFS_MAIN -->|Check Directory Metadata| INMEMORY_CACHE
    GHFS_MAIN -->|Authenticate| JSON_KEY
    GHFS_MAIN -->|Federated OAuth Token| WORKLOAD_ID
    WORKLOAD_ID -->|Generate Token| GCP_IAM

    GHFS_MAIN -->|HTTPS / HTTP2 gRPC| GCS_API
    GCS_API --> BUCKET
```

---

## 2. GCS Coalesced Read & Prefetching Mechanics

```mermaid
sequenceDiagram
    autonumber
    participant Spark as Spark Vectorized Parquet Reader
    participant GCS_FS as GoogleHadoopFileSystem (GHFS)
    participant GCS as GCS REST API (storage.googleapis.com)

    Spark->>GCS_FS: Read Column 1 (Offset 100-500) & Column 5 (Offset 5000-5500)
    Note over GCS_FS: GCS Connector analyzes request gap (4500 bytes)
    GCS_FS->>GCS: Single HTTP GET Object with Range: bytes=100-5500
    GCS-->>GCS_FS: 206 Partial Content (Single HTTP connection stream)
    GCS_FS->>Spark: Deliver Column 1 & Buffer Column 5 in memory
```
