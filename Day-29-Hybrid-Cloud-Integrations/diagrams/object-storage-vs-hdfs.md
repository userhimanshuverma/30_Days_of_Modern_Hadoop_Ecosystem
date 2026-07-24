# Object Storage vs HDFS Comparison Diagrams

Architectural breakdown comparing block-based HDFS with Key-Value Object Storage.

---

## 1. Physical vs Logical Storage Structure

```mermaid
graph TB
    subgraph HDFS_Architecture ["Hadoop Distributed File System (HDFS)"]
        NN["NameNode - In-Memory Namespace & Block Map"]
        DN1["DataNode 1 - Block 101: 128MB File Chunk"]
        DN2["DataNode 2 - Block 102: 128MB File Chunk"]
        DN3["DataNode 3 - Block 103: Replica of Block 101"]
        
        NN -->|Block Report| DN1
        NN -->|Block Report| DN2
        NN -->|Block Report| DN3
    end

    subgraph Object_Storage_Architecture ["Cloud Object Storage (S3 / ADLS / GCS)"]
        BUCKET["Bucket / Container Namespace"]
        OBJ1["Key: 'raw/year=2026/file.parquet' (Blob Metadata + Data)"]
        OBJ2["Key: 'raw/year=2026/file2.parquet' (Blob Metadata + Data)"]
        
        BUCKET --> OBJ1
        BUCKET --> OBJ2
    end
```

---

## 2. Directory Listing vs Prefix Scan Mechanics

```mermaid
sequenceDiagram
    autonumber
    participant App as Analytics Engine
    participant HDFS as HDFS NameNode
    participant S3 as AWS S3 / Object Store

    Note over App, HDFS: HDFS Directory Listing (O(1) In-Memory Lookup)
    App->>HDFS: getListing('/user/hive/warehouse/sales')
    HDFS-->>App: Instant response with array of FileStatus objects (In-Memory Data Structure)

    Note over App, S3: S3 Prefix Scan (O(N) HTTP Pagination over Keys)
    App->>S3: GET /bucket?prefix=user/hive/warehouse/sales/&max-keys=1000
    S3-->>App: Page 1 (1,000 keys + NextContinuationToken)
    App->>S3: GET /bucket?prefix=user/hive/warehouse/sales/&continuation-token=...
    S3-->>App: Page 2 (1,000 keys)
```
