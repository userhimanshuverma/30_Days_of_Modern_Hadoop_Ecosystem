# Hands-On Lab: Deploying and Validating Secure HDFS (Kerberos, TLS, ACLs, and TDE)

Welcome to the hands-on lab for **Day 4 — HDFS Security & Encryption**. In this lab, you will deploy a fully functional, self-contained multi-node Hadoop cluster running in Docker. The environment integrates MIT Kerberos for authentication, TLS for transport security (HTTPS and secure IPC), HDFS Extended Access Control Lists (ACLs) for granular authorization, and a Hadoop Key Management Server (KMS) to facilitate Transparent Data Encryption (TDE) at rest.

---

## Lab Objectives
By the end of this lab, you will have completed the following exercises:
1. **Bootstrap a Secure Cluster**: Build and run MIT KDC, KMS, NameNode, DataNode, and Client containers.
2. **Retrieve Kerberos Tickets**: Authenticate users using keytabs (`kinit`) and inspect ticket caches.
3. **Verify SSL/TLS Encryption**: Verify NameNode and KMS Web UI services over HTTPS using signed certificates.
4. **Enforce Fine-Grained Access Controls**: Set and test HDFS POSIX permissions and Extended Access Control Lists (ACLs).
5. **Implement TDE (Encryption at Rest)**: Create KMS keys, provision HDFS Encryption Zones, write files, and prove the raw data block is encrypted on the DataNode physical disk.
6. **Teardown & Cleanup**: Remove local container mounts and networks safely.

---

## Prerequisites
- **Docker Engine**: v20.10+ installed and running.
- **Docker Compose**: v2.0+ installed.
- **System Memory**: Minimum 4 GB RAM allocated to Docker.

---

## Step 1: Deploy the Lab Environment

First, navigate to the `docker/` directory of this module and boot the container suite.

```bash
cd docker/
docker compose up -d
```

### What this does behind the scenes:
1. **`kdc-server`**: Spins up Alpine Linux, starts the MIT Kerberos database, registers service principals for HDFS and KMS, generates keytabs, and deposits them into a shared volume.
2. **`namenode` & `datanode`**: Starts CentOS-based Hadoop daemons. The NameNode triggers `generate-certs.sh` to construct a local CA, certificates, and JKS stores, then formats the filesystem, reads `nn.keytab` and starts up. The DataNode reads `dn.keytab`, waits for certificates, and starts up using secure RPC channels.
3. **`kms-server`**: Starts the Key Management service listening on HTTPS port `9600`.
4. **`client`**: Boots a node pre-packaged with Kerberos client libraries and maps validation scripts into the volume.

Validate that all 5 containers are running:
```bash
docker compose ps
```

---

## Step 2: Access the Client Container

All commands in this lab should be run from inside the `client` container. Open an interactive shell on the client container:

```bash
docker exec -it docker-client-1 bash
```
*(Note: depending on your Docker version, the container name might be `docker-client-1` or `docker_client_1` or simply `client`. Run `docker ps` to verify the active name).*

---

## Step 3: Exercise 1 - Kerberos Authentication (`kinit` & `klist`)

Inside the client container, try writing to the NameNode without authenticating:

```bash
hdfs dfs -ls hdfs://namenode.hadoop.local:9000/
```
**Expected Output:**
```text
WARN ipc.Client: Exception encountered while connecting to the server : org.apache.hadoop.security.AccessControlException: Client principal is null
```

Now, authenticate as the regular user `alice` using her keytab:

```bash
kinit -kt /etc/security/keytabs/alice.keytab alice@HADOOP.LOCAL
```

Verify that the ticket was added to your local cache:

```bash
klist
```
**Expected Output:**
```text
Ticket cache: FILE:/tmp/krb5cc_0
Default principal: alice@HADOOP.LOCAL

Valid starting       Expires              Service principal
06/25/2026 18:00:00  06/26/2026 18:00:00  krbtgt/HADOOP.LOCAL@HADOOP.LOCAL
```

Now try listing the HDFS directory again:
```bash
hdfs dfs -ls /
```
You can now access HDFS!

---

## Step 4: Exercise 2 - Verify SSL/TLS (HTTPS)

Verify that the NameNode and KMS Web UIs are operating over TLS. 

Since we generated certificates signed by our own local Certificate Authority, the CA certificate was imported into the client's OS trust store during bootstrap. We can test connections using standard `curl`.

1. **Verify NameNode HTTPS Web UI Port 9871**:
   ```bash
   curl -I https://namenode.hadoop.local:9871/
   ```
   **Expected Output:**
   ```text
   HTTP/1.1 200 OK
   Cache-Control: no-cache
   Content-Type: text/html; charset=utf-8
   ```

2. **Verify KMS HTTPS REST Port 9600**:
   ```bash
   curl -I https://kms-server.hadoop.local:9600/kms/index.html
   ```
   **Expected Output:**
   ```text
   HTTP/1.1 200 OK
   Content-Type: text/html
   ```

---

## Step 5: Exercise 3 - Configure HDFS Access Control Lists (ACLs)

HDFS supports extended access control lists for users who are neither the owner nor group members of a directory.

1. **Authenticate as the HDFS superuser (`hdfs`)**:
   ```bash
   kinit -kt /etc/security/keytabs/hdfs.keytab hdfs@HADOOP.LOCAL
   ```

2. **Create a secure directory**:
   ```bash
   hdfs dfs -mkdir /payroll
   hdfs dfs -chmod 700 /payroll
   ```

3. **Verify Alice has no access**:
   Switch back to alice:
   ```bash
   kinit -kt /etc/security/keytabs/alice.keytab alice@HADOOP.LOCAL
   hdfs dfs -ls /payroll
   ```
   **Expected Output:**
   ```text
   ls: Permission denied: user=alice, access=READ_EXECUTE, path="/payroll":hdfs:supergroup:d---------
   ```

4. **Grant Alice Read-Execute access via Extended ACLs**:
   Switch back to hdfs, apply the ACL, and read it back:
   ```bash
   kinit -kt /etc/security/keytabs/hdfs.keytab hdfs@HADOOP.LOCAL
   hdfs dfs -setfacl -m user:alice:r-x /payroll
   hdfs dfs -getfacl /payroll
   ```
   **Expected Output:**
   ```text
   # file: /payroll
   # owner: hdfs
   # group: supergroup
   user::rwx
   user:alice:r-x
   group::---
   mask::r-x
   other::---
   ```

5. **Test Alice's access again**:
   ```bash
   kinit -kt /etc/security/keytabs/alice.keytab alice@HADOOP.LOCAL
   hdfs dfs -ls /payroll
   ```
   Success! Alice can list `/payroll` despite its core permissions being `700`.

---

## Step 6: Exercise 4 - Provision Transparent Data Encryption (TDE)

Now we will establish an HDFS Encryption Zone to secure data at rest.

1. **Create the KMS Key (Run as `hdfs` admin)**:
   ```bash
   kinit -kt /etc/security/keytabs/hdfs.keytab hdfs@HADOOP.LOCAL
   hadoop key create payroll-key
   ```

2. **Establish the HDFS Encryption Zone**:
   Create a directory and turn it into an Encryption Zone backed by the KMS key:
   ```bash
   hdfs dfs -mkdir /payroll/encrypted-payroll
   hdfs crypto -createZone -keyName payroll-key -path /payroll/encrypted-payroll
   ```

3. **Grant Write ACL permissions to Alice**:
   ```bash
   hdfs dfs -setfacl -m user:alice:rwx /payroll/encrypted-payroll
   ```

4. **Write Confidential Data (Run as `alice`)**:
   ```bash
   kinit -kt /etc/security/keytabs/alice.keytab alice@HADOOP.LOCAL
   echo "CRITICAL-BANKING-TRANSFERS-2026" | hdfs dfs -put - /payroll/encrypted-payroll/transfers.csv
   ```

5. **Verify Decryption works on read**:
   ```bash
   hdfs dfs -cat /payroll/encrypted-payroll/transfers.csv
   ```
   *Output: `CRITICAL-BANKING-TRANSFERS-2026`* (Decrypted transparently by client talking to KMS).

6. **Prove the block file is encrypted on disk**:
   Find the block ID assigned by NameNode:
   ```bash
   hdfs fsck /payroll/encrypted-payroll/transfers.csv -files -blocks
   ```
   Look for the block ID string, e.g., `blk_1073741825`.
   
   Open a separate shell on your **host machine** and search the DataNode volume for the raw block file:
   ```bash
   docker exec -it docker-datanode-1 find /var/lib/hadoop/dfs/data/current/ -name "blk_1073741825"
   ```
   Read the raw file on the DataNode:
   ```bash
   docker exec -it docker-datanode-1 cat /var/lib/hadoop/dfs/data/current/.../finalized/subdir0/subdir0/blk_1073741825
   ```
   **Result**: The output contains garbled binary ciphertext! It does **not** contain the words `CRITICAL-BANKING-TRANSFERS-2026`. This proves the data was encrypted at the client before writing to HDFS, and the DataNode only stored ciphertext.

---

## Step 7: Automated Master Security Runner

You can execute all the above checks automatically using the verification suite:

```bash
bash /tmp/scripts/verify-security.sh
```

If everything is configured correctly, it will end with:
```text
================================================================
🏆 SUCCESS: ALL SECURE HADOOP COMPLIANCE CHECKS PASSED!
================================================================
```

---

## Step 8: Cleanup

When you are finished with the lab, exit the container and run the cleanup sequence on your host:

```bash
exit
docker compose down -v
```
This stops the services and purges the volumes containing keys, certificates, and HDFS blocks.
