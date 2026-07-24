# Cloud Storage Production Troubleshooting Playbook

Enterprise troubleshooting guide for diagnosing, resolving, and preventing production issues across Hadoop S3A, Azure ABFS, and Google GCS connectors.

---

## 1. Authentication Failures & Access Denied

### Symptoms
- Spark/Hive jobs fail with `org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException: No AWS Credentials provided`.
- `AccessDeniedException` (HTTP 403 Forbidden) when listing or reading prefixes.

### Root Cause
1. Mismatched or missing credential provider implementation class in `core-site.xml`.
2. Expired IAM role session tokens, revoked Service Principal secret keys, or incorrect OIDC JWT mounts.
3. IAM policy lacks `s3:ListBucket` or `s3:GetObject` permission on the specific bucket resource path.

### Logs
```text
Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 9A81F722B001)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1810)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:254)
```

### Resolution
1. Verify credential provider chain order in `core-site.xml`:
   ```xml
   <property>
       <name>fs.s3a.aws.credentials.provider</name>
       <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.amazonaws.auth.EnvironmentVariableCredentialsProvider,com.amazonaws.auth.InstanceProfileCredentialsProvider</value>
   </property>
   ```
2. Test permissions using CLI:
   `aws s3 ls s3a://your-bucket/` or `az storage blob list --account-name youraccount`.

### Preventive Measures
- Transition from static access keys to IRSA (IAM Roles for Service Accounts) or Azure Managed Identities.
- Implement automated key rotation with HashiCorp Vault.

---

## 2. Invalid Credentials

### Symptoms
- `com.amazonaws.services.s3.model.AmazonS3Exception: The security token included in the request is invalid` (HTTP 400/401).
- `AADSTS7000215: Invalid client secret provided`.

### Root Cause
1. Secret keys contain unescaped special characters (e.g. `/`, `+`, `=`) in `core-site.xml` or environment variables.
2. System clock drift between cluster nodes and cloud NTP servers (> 15 minutes clock skew).

### Logs
```text
com.amazonaws.services.s3.model.AmazonS3Exception: The Difference Between The Request Time And The Current Time Is Too Large (Status Code: 403; Error Code: RequestTimeTooSkewed)
```

### Resolution
1. Sync node hardware clocks via NTP: `sudo systemctl restart systemd-timesyncd` or `ntpdate pool.ntp.org`.
2. URL-encode special characters in connection strings or pass keys via secure environment variables.

### Preventive Measures
- Enforce cluster-wide NTP synchronization with chrony.
- Avoid passing raw secrets in plain-text XML files.

---

## 3. Slow Uploads & Network Throughput Degraded

### Symptoms
- Spark tasks writing Parquet files take 10x longer on S3/ADLS than on local HDFS.
- Network interface utilization drops to < 5% during output commit phase.

### Root Cause
1. Single-threaded write buffering without fast upload disk spooling (`fs.s3a.fast.upload=false`).
2. Small multipart block size causing thousands of HTTP part upload requests.

### Logs
```text
INFO s3a.S3ABlockOutputStream: Uploading block 4 of part part-0.parquet without fast-upload buffer
WARN s3a.S3AFileSystem: Serial upload execution slowing down task output write throughput
```

### Resolution
Enable fast upload disk buffering and increase part size in `core-site.xml`:
```xml
<property>
    <name>fs.s3a.fast.upload</name>
    <value>true</value>
</property>
<property>
    <name>fs.s3a.fast.upload.buffer</name>
    <value>disk</value>
</property>
<property>
    <name>fs.s3a.multipart.size</name>
    <value>104857600</value> <!-- 100MB -->
</property>
```

### Preventive Measures
- Provision NVMe drives for `/tmp` fast upload disk buffer directories on worker nodes.

---

## 4. Multipart Upload Failures & Leaks

### Symptoms
- Cloud storage bill ballooning despite small visible dataset sizes.
- Spark tasks fail with `org.apache.hadoop.fs.s3a.S3AIOException: Multi-part upload to table/part-0.parquet failed`.

### Root Cause
Executor containers killed abruptly due to YARN OOM (Out Of Memory) or spot instance termination leave abandoned multipart upload fragments stored in S3.

### Logs
```text
ERROR s3a.S3ABlockOutputStream: Unable to complete multipart upload mp-88129: Part 3 ETag mismatch
com.amazonaws.services.s3.model.AmazonS3Exception: The specified upload does not exist (Status Code: 404; Error Code: NoSuchUpload)
```

### Resolution
1. Set S3 Lifecycle rule to abort incomplete multipart uploads after 7 days:
   `aws s3api put-bucket-lifecycle-configuration --bucket my-lake-bucket --lifecycle-configuration file://lifecycle.json`
2. Run manual cleanup command:
   `hadoop s3guard uploads -abort -age 7d s3a://my-lake-bucket/`

### Preventive Measures
- Configure S3 bucket lifecycle rules upon bucket creation in Terraform / CloudFormation.

---

## 5. Connection & Socket Timeouts

### Symptoms
- Spark jobs fail with `java.net.SocketTimeoutException: Read timed out` during long-running table writes or prefix scans.

### Root Cause
Network socket idle timeout value (`fs.s3a.connection.timeout`) is lower than the time required for S3 to process deep prefix listings or heavy write buffers.

### Logs
```text
Caused by: java.net.SocketTimeoutException: timeout
  at okhttp3.internal.http2.Http2Stream$StreamTimeout.newTimeoutException(Http2Stream.java:666)
  at org.apache.hadoop.fs.s3a.S3AInputStream.read(S3AInputStream.java:312)
```

### Resolution
Increase socket and connection timeout settings in `core-site.xml`:
```xml
<property>
    <name>fs.s3a.connection.timeout</name>
    <value>200000</value> <!-- 200 seconds -->
</property>
<property>
    <name>fs.s3a.connection.establish.timeout</name>
    <value>30000</value>
</property>
<property>
    <name>fs.s3a.attempts.maximum</name>
    <value>20</value>
</property>
```

### Preventive Measures
- Adjust socket timeouts proportionally when working over high-latency cross-region lines.

---

## 6. Bucket Permission & Policy Conflicts

### Symptoms
- IAM user has `AdministratorAccess`, but Spark jobs fail with `AccessDenied` (HTTP 403) when attempting to write to S3 bucket.

### Root Cause
Explicit `DENY` statement in the S3 Bucket Policy (e.g. enforcing TLS `aws:SecureTransport` or KMS key enforcement) overriding IAM role permissions.

### Logs
```text
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Status Code: 403; Error Code: AccessDenied; Request ID: XYZ123)
Bucket Policy Condition Failed: aws:SecureTransport is false
```

### Resolution
1. Enforce HTTPS in `core-site.xml`:
   ```xml
   <property>
       <name>fs.s3a.connection.ssl.enabled</name>
       <value>true</value>
   </property>
   ```
2. Inspect bucket policy via AWS CLI:
   `aws s3api get-bucket-policy --bucket my-lake-bucket`

### Preventive Measures
- Validate bucket policies against organizational security baselines using AWS Access Analyzer.

---

## 7. DNS Failures & Name Resolution Errors

### Symptoms
- Hadoop jobs fail with `java.net.UnknownHostException: my-lake-bucket.s3.us-east-1.amazonaws.com`.

### Root Cause
NodeManager host DNS resolver (e.g. `/etc/resolv.conf` or CoreDNS) failing to resolve cloud storage endpoints due to VPC DNS rate limiting or broken VPN tunnels.

### Logs
```text
java.net.UnknownHostException: s3.us-east-1.amazonaws.com: Name or service not known
  at java.net.Inet6AddressImpl.lookupAllHostAddr(Native Method)
  at java.net.InetAddress$2.lookupAllHostAddr(InetAddress.java:929)
```

### Resolution
1. Verify host DNS resolution: `dig s3.us-east-1.amazonaws.com` or `nslookup account.dfs.core.windows.net`.
2. Configure local DNS caching daemon (`systemd-resolved` or `dnsmasq`) on cluster nodes.

### Preventive Measures
- Use VPC Interface Endpoints (AWS PrivateLink) for fixed internal IP routing.

---

## 8. SSL & Certificate Validation Issues

### Symptoms
- `javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException`.

### Root Cause
Corporate proxy, SSL inspection firewall, or outdated JDK Java KeyStore (`cacerts`) missing cloud root CA certificates (e.g. Amazon Root CA 1 / DigiCert Global Root CA).

### Logs
```text
javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
  at sun.security.ssl.Alert.createSSLException(Alert.java:131)
```

### Resolution
Import root CA into JDK keytool:
```bash
keytool -importcert -trustcacerts -alias amazonrootca1 \
  -file AmazonRootCA1.crt -keystore $JAVA_HOME/lib/security/cacerts \
  -storepass changeit -noprompt
```

### Preventive Measures
- Keep JDK security certificates updated across all cluster node images.

---

## 9. Connector Version & Dependency Mismatches

### Symptoms
- Spark job fails at startup with `java.lang.ClassNotFoundException: com.amazonaws.services.s3.model.S3Object` or `java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument`.

### Root Cause
Classpath collision between Hadoop's `hadoop-aws` module, `aws-java-sdk-bundle.jar`, and Spark's internal Guava or Jackson library versions.

### Logs
```text
java.lang.NoSuchMethodError: com.fasterxml.jackson.databind.ObjectMapper.readTree(Ljava/lang/String;)Lcom/fasterxml/jackson/databind/JsonNode;
  at com.amazonaws.internal.config.InternalConfig$Factory.create(InternalConfig.java:319)
```

### Resolution
1. Ensure matching versions of Hadoop AWS and AWS SDK:
   - Hadoop 3.3.4 requires `aws-java-sdk-bundle-1.12.262.jar`.
2. Submit jobs using `--packages` or shaded fat JARs:
   `spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 ...`

### Preventive Measures
- Utilize shaded connector JARs (`gcs-connector-hadoop3-shaded.jar`) to isolate third-party dependencies.

---

## 10. Throttling & HTTP 503 SlowDown / HTTP 429

### Symptoms
- High task retry rate in YARN/Spark UI.
- Tasks fail with `com.amazonaws.services.s3.model.AmazonS3Exception: SlowDown (Status Code: 503; Error Code: SlowDown)`.

### Root Cause
Exceeding request throughput limits per prefix:
- AWS S3 limits: 3,500 PUT/POST/DELETE and 5,500 GET requests per second per prefix.
- GCS limits: 1,000 write requests per second per bucket.

### Logs
```text
org.apache.hadoop.fs.s3a.S3AIOException: getFileStatus s3a://warehouse/partition_dt=2026-07-24/: 
com.amazonaws.services.s3.model.AmazonS3Exception: SlowDown (Status Code: 503; Error Code: SlowDown)
```

### Resolution
1. **Hash Partitioning / Entropy Prefixes**: Introduce hash prefixes to distribute objects across S3 partitions (e.g. `s3a://bucket/a1f8-data/year=2026/`).
2. Increase exponential backoff retry parameters in `core-site.xml`:
   ```xml
   <property>
       <name>fs.s3a.retry.limit</name>
       <value>20</value>
   </property>
   <property>
       <name>fs.s3a.retry.interval</name>
       <value>500ms</value>
   </property>
   ```

### Preventive Measures
- Migrate to Apache Iceberg or Delta Lake formats to eliminate prefix scans entirely.
