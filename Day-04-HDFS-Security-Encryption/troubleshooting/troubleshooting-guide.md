# Production Troubleshooting Playbook: HDFS Security & Encryption

This guide compiles production-level recovery runbooks for HDFS security and encryption failures. Each scenario details the specific symptoms, root causes, relevant logs, diagnosis procedures, and final resolution.

---

## Scenario 1: Kerberos Ticket Expired

### Symptoms
Hadoop CLI commands fail, returning authentication exceptions. Long-running streaming jobs (such as Spark Structured Streaming or Flink) or MapReduce applications abruptly crash after 24 hours.

```text
ERROR ipc.Client: Failed to connect to server: namenode/172.18.0.3:9000:
org.apache.hadoop.security.AccessControlException: Client principal is null
- OR -
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided]
```

### Root Cause
Kerberos credentials exist in a temporary ticket cache in memory. Tickets expire (standard lifetime is 24 hours). If client processes do not refresh the ticket, or if the ticket lifetime threshold passes without a valid renewal, RPC calls are rejected by Hadoop daemons.

### Diagnostics & Logs
Check client active credentials:
```bash
klist
```
Look at the expiration dates. If the current time is past the `Expires` field, the ticket is stale.

Inspect Hadoop user logs (client-side or container logs at `/var/log/hadoop/hadoop.log`):
```text
DEBUG security.UserGroupInformation: PrivilegedActionException:
  org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException):
  Authentication required
```

### Resolution
1. **Interactive Users**: Simply run `kinit` again using the keytab:
   ```bash
   kinit -kt /etc/security/keytabs/alice.keytab alice@HADOOP.LOCAL
   ```
2. **Long-Running Services**: Ensure that services use keytabs and that Hadoop's background renewal threads are running. Configure the renewal interval in `core-site.xml`:
   ```xml
   <property>
     <name>hadoop.kerberos.keytab.login.autorenewal.enabled</name>
     <value>true</value>
   </property>
   ```
3. Ensure the service utilizes standard UserGroupInformation API login patterns (e.g. `UserGroupInformation.loginUserFromKeytab(principal, keytab)`), which schedules automatic ticket renewal.

---

## Scenario 2: Invalid Principal or Auth-to-Local Rule Failures

### Symptoms
Kerberos login works (`kinit` returns 0), but communicating with NameNode fails with:
```text
org.apache.hadoop.security.AccessControlException: 
Your principal 'alice/client.hadoop.local@HADOOP.LOCAL' cannot be mapped to a local operating system user.
```

### Root Cause
Hadoop translates Kerberos principals (e.g., `nn/namenode.hadoop.local@HADOOP.LOCAL`) into local OS user accounts (like `hdfs`) using rules defined by `hadoop.security.auth_to_local` in `core-site.xml`. If the principal does not match any rule, Hadoop cannot resolve a local username and terminates the request.

### Diagnostics & Logs
Check `/var/log/hadoop/hadoop-hdfs-namenode.log`:
```text
WARN org.apache.hadoop.security.UserGroupInformation: No rule matches principal alice/client.hadoop.local@HADOOP.LOCAL
```

### Resolution
1. Test rule resolution on the command line using:
   ```bash
   hadoop org.apache.hadoop.security.HadoopKerberosName alice/client.hadoop.local@HADOOP.LOCAL
   ```
2. Update the `hadoop.security.auth_to_local` rules in `core-site.xml` to match the principal format. For example, to map `alice/client.hadoop.local@HADOOP.LOCAL` to `alice`, add:
   ```xml
   <property>
     <name>hadoop.security.auth_to_local</name>
     <value>
       RULE:[2:$1@$0](alice/.*@HADOOP.LOCAL)s/.*/alice/
       DEFAULT
     </value>
   </property>
   ```

---

## Scenario 3: POSIX Permission or Extended ACL Denials

### Symptoms
Clients encounter:
```text
Permission denied: user=alice, access=WRITE, path="/data/reports/january.csv":hdfs:finance:drwxr-xr-x
```

### Root Cause
The client principal has authenticated successfully, but the HDFS permissions checker blocked the action. Standard POSIX permissions check: owner (user), group membership, and others. If extended ACLs are enabled, HDFS evaluates named user entries, named group entries, and masks.

### Diagnostics & Logs
Verify user identities and group mappings on HDFS:
```bash
hdfs groups alice
```
Check HDFS permissions and extended ACLs on the target file/folder:
```bash
hdfs dfs -getfacl /data/reports/january.csv
```

NameNode audit logs (`/var/log/hadoop/hdfs-audit.log`) show:
```text
allowed=false ugi=alice (auth:KERBEROS) ip=/172.18.0.6 cmd=create src=/data/reports/january.csv dst=null perm=hdfs:finance:drwxr-xr-x
```

### Resolution
1. Adjust standard permissions:
   ```bash
   hdfs dfs -chmod 775 /data/reports/january.csv
   ```
2. Apply an extended ACL granting specific user access:
   ```bash
   hdfs dfs -setfacl -m user:alice:rwx /data/reports/january.csv
   ```

---

## Scenario 4: SSL/TLS Handshake Failures

### Symptoms
Client connections fail when contacting NameNode HTTPS (9871) or KMS (9600):
```text
curl: (60) SSL certificate problem: unable to get local issuer certificate
- OR -
javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
```

### Root Cause
The server is presenting an SSL certificate that is not trusted by the client. This occurs if using self-signed certificates, if the local Certificate Authority (CA) root certificate is missing from the client's Java truststore (`truststore.jks`) or host certificate bundle, or if there is a hostname mismatch in the certificate Common Name (CN)/Subject Alternative Name (SAN).

### Diagnostics & Logs
Inspect SSL certificate handshakes using `openssl`:
```bash
openssl s_client -connect namenode.hadoop.local:9871 -showcerts
```
Check if the certificate is signed by your custom CA.

Check client-side Java logs (enable JSSE debugging):
```bash
export HADOOP_OPTS="-Djavax.net.debug=ssl,handshake"
hdfs dfs -ls https://namenode.hadoop.local:9871/
```

### Resolution
1. **System Level (curl/openssl)**: Import the CA root certificate into the local store:
   ```bash
   cp ca-cert.pem /etc/pki/ca-trust/source/anchors/hadoop-ca.crt
   update-ca-trust
   ```
2. **Java Level (Hadoop daemons/clients)**: Confirm truststore paths and passwords in `ssl-client.xml`:
   ```xml
   <property>
     <name>ssl.client.truststore.location</name>
     <value>/var/ssl/truststore.jks</value>
   </property>
   ```
3. Regenerate certificates ensuring the CN contains the matching host wildcard (e.g., `*.hadoop.local`).

---

## Scenario 5: HDFS Encryption Zone (TDE) Write/Read Errors

### Symptoms
Writing or reading files inside an Encryption Zone (EZ) fails:
```text
org.apache.hadoop.crypto.key.KeyProviderTokenIssuer$1: KMS provider connection failed.
- OR -
org.apache.hadoop.security.AccessControlException: Permission denied: User [alice] does not have [DECRYPT_EEK] privilege on key [payroll-key]
```

### Root Cause
To read/write inside an encryption zone, the client must contact the KMS. The NameNode requests an Encrypted Encryption Key (EEK), and the KMS decrypts the EEK using the master key. If the client user does not have `DECRYPT_EEK` permissions in `kms-acls.xml`, or if the KMS is unreachable, HDFS operations abort.

### Diagnostics & Logs
Check KMS Access Logs at `/var/log/hadoop/kms-audit.log`:
```text
OK[DECRYPT_EEK] User=alice Key=payroll-key
- OR -
UNAUTHORIZED[DECRYPT_EEK] User=alice Key=payroll-key
```

Check NameNode logs (`/var/log/hadoop/hadoop-hdfs-namenode.log`):
```text
IOException: Encrypted Key decryption failed due to insufficient KMS authorization
```

### Resolution
1. Verify the client has access in the KMS ACL configurations (`kms-acls.xml`):
   ```xml
   <property>
     <name>key.acl.payroll-key.DECRYPT_EEK</name>
     <value>alice</value>
   </property>
   ```
2. Reload KMS configurations:
   ```bash
   hadoop kmsadmin -reload
   ```

---

## Scenario 6: Missing Keytabs on Node Startup

### Symptoms
Hadoop services fail to start. Running `jps` shows that NameNode or DataNode processes terminate immediately.

### Root Cause
Hadoop configuration files (`hdfs-site.xml`) point to a keytab file location that either does not exist on disk, is not accessible due to file permissions, or lacks the principal matching the service hostname.

### Diagnostics & Logs
Check service startup logs (e.g., `/var/log/hadoop/hadoop-hdfs-namenode.log`):
```text
FATAL org.apache.hadoop.hdfs.server.namenode.NameNode: Failed to start namenode.
java.io.IOException: Login failure for nn/namenode.hadoop.local@HADOOP.LOCAL from keytab /etc/security/keytabs/nn.keytab:
java.io.FileNotFoundException: /etc/security/keytabs/nn.keytab (Permission denied)
```

### Resolution
1. Verify keytab existence and read permissions:
   ```bash
   ls -la /etc/security/keytabs/nn.keytab
   ```
   Ensure the Hadoop process owner (e.g., `hdfs` user or `root`) has read permissions.
2. Confirm the keytab contains the correct principal:
   ```bash
   klist -kt /etc/security/keytabs/nn.keytab
   ```
   Ensure it list `nn/namenode.hadoop.local@HADOOP.LOCAL` and `HTTP/namenode.hadoop.local@HADOOP.LOCAL`.

---

## Scenario 7: Hadoop Key Management Server (KMS) Unavailable

### Symptoms
Writing or reading from encryption zones fails with:
```text
java.io.IOException: KMS provider connection failed: ConnectException: Connection refused
```

### Root Cause
The KMS daemon is stopped, listening on the wrong port/IP, or blocked by local firewalls.

### Diagnostics & Logs
On the KMS host, check port listening status:
```bash
netstat -tulpn | grep 9600
```

Check the KMS console logs:
```text
FATAL: KeyManagementServerException: java.net.BindException: Address already in use
```

Verify KMS connectivity from the NameNode using curl:
```bash
curl -k https://kms-server.hadoop.local:9600/kms/v1/keys
```

### Resolution
1. Start the KMS service:
   ```bash
   kms.sh start
   ```
2. Verify KMS port bindings in `kms-site.xml`:
   ```xml
   <property>
     <name>hadoop.kms.https.port</name>
     <value>9600</value>
   </property>
   ```
3. Make sure DNS resolution for `kms-server.hadoop.local` is working.

---

## Scenario 8: Ranger Policy Authorization Failures

### Symptoms
Kerberos auth and KMS are online, but access to HDFS commands is blocked:
```text
org.apache.hadoop.security.AccessControlException: 
Permission denied: user=alice, access=READ, path="/data/sensitive.csv" (Blocked by Ranger)
```

### Root Cause
Apache Ranger is configured as the authorization plugin for HDFS, overriding POSIX/ACL checks on the NameNode. The Ranger plugin did not download the latest access policy database, or no policy exists permitting the principal `alice` to perform the read on `/data/sensitive.csv`.

### Diagnostics & Logs
Look at Ranger plugin cache status on the NameNode:
```bash
ls -l /etc/ranger/hdfs/policycache/
```
Verify the cache timestamp is fresh.

Search `/var/log/hadoop/hadoop-hdfs-namenode.log` for Ranger plugins:
```text
DEBUG org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer:
  RangerHdfsAuthorizer.checkPermission: perm=READ path=/data/sensitive.csv result=denied
```

### Resolution
1. Log in to the Apache Ranger Admin console.
2. In the HDFS repository service manager, add or update a policy for the path `/data/sensitive.csv`.
3. Explicitly add user `alice` to the list of allowed readers.
4. Trigger policy sync or wait for the automatic sync interval (default: 30 seconds).
