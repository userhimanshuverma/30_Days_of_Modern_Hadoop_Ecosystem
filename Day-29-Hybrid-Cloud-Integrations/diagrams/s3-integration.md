# AWS S3 Integration Diagrams

Detailed diagrams for AWS S3 and S3A connector internals including S3A Magic Committers and Directory Committers.

---

## 1. S3A Connector Internal Architecture

```mermaid
graph TD
    subgraph Hadoop_Application_Layer ["Hadoop / Spark / Hive Execution Layer"]
        APP["Spark Task / MapReduce Worker"]
    end

    subgraph S3A_FileSystem_Layer ["org.apache.hadoop.fs.s3a.S3AFileSystem"]
        S3A_FS["S3AFileSystem Entry Point"]
        CRED_PROV["AWSCredentialsProvider Chain"]
        INPUT_STREAM["S3AInputStream - Vectorized & Range Reads"]
        OUTPUT_STREAM["S3AOutputStream - Fast Upload Buffer"]
        COMMITTER["S3A Magic Committer / Directory Committer"]
    end

    subgraph AWS_SDK_V2 ["AWS SDK for Java v2"]
        AWS_CLIENT["S3Client / AsyncS3Client"]
        HTTP_CLIENT["Netty / CRT HTTP Client Pool"]
    end

    subgraph AWS_S3_Cloud ["AWS S3 Endpoint"]
        S3_REST["AWS S3 REST Service (s3.amazonaws.com)"]
    end

    APP -->|fs.open / fs.create| S3A_FS
    S3A_FS -->|Resolve Credentials| CRED_PROV
    S3A_FS -->|Read Stream| INPUT_STREAM
    S3A_FS -->|Write Stream| OUTPUT_STREAM
    S3A_FS -->|Commit Task Writes| COMMITTER

    OUTPUT_STREAM -->|InitiateMultipartUpload / UploadPart| AWS_CLIENT
    INPUT_STREAM -->|GetObject Range| AWS_CLIENT
    AWS_CLIENT -->|Connection Reuse / Retries| HTTP_CLIENT
    HTTP_CLIENT -->|HTTPS REST V4 Sig| S3_REST
```

---

## 2. S3A Magic Committer Zero-Rename Architecture

```mermaid
sequenceDiagram
    autonumber
    participant Task as Spark Executor Task
    participant Magic as S3A Magic Committer
    participant S3 as AWS S3 API
    participant Driver as Spark Driver (Job Master)

    Note over Task, S3: 1. Task Writes Data (Without O(N) Renames)
    Task->>Magic: Write record to s3a://bucket/table/__magic/job1/task1/part-0.parquet
    Magic->>S3: InitiateMultipartUpload(Target: s3a://bucket/table/part-0.parquet)
    S3-->>Magic: Return UploadId: "upload-abc-123"
    Task->>Magic: Stream data chunks (64MB buffer)
    Magic->>S3: UploadPart(UploadId, Part 1, Data)
    S3-->>Magic: Return ETag: "etag-part-1"
    
    Note over Task, Driver: 2. Task Commit Phase
    Task->>Magic: commitTask()
    Magic->>S3: Write .pendingset manifest to S3 (contains UploadId + ETags)
    Magic-->>Driver: Send TaskCommitMessage containing pending uploads
    
    Note over Driver, S3: 3. Job Commit Phase (Zero Data Movement!)
    Driver->>Magic: commitJob()
    Driver->>S3: CompleteMultipartUpload(UploadId, ETags)
    S3-->>Driver: 200 OK (Data instantly visible in destination directory!)
```
