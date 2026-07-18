# Apache Ranger & Apache Atlas Production Troubleshooting Playbook

This document details root cause analysis and resolution steps for common issues encountered when managing Ranger policies and Atlas metadata integrations.

---

## 🛠️ Diagnostics Map & Logs Locations

When debugging authorization or metadata issues, look at the following log locations:

| Component | Log File Path | Key Debug Levels |
| :--- | :--- | :--- |
| **Ranger Admin** | `/var/log/ranger/admin/xa_portal.log` | `log4j.logger.org.apache.ranger=DEBUG` |
| **Atlas Server** | `/var/log/atlas/application.log` | `log4j.logger.org.apache.atlas=DEBUG` |
| **HDFS Namenode** | `/var/log/hadoop/hdfs/hadoop-hdfs-namenode.log` | `log4j.logger.org.apache.ranger=DEBUG` |
| **HiveServer2** | `/var/log/hive/hiveserver2.log` | `log4j.logger.org.apache.atlas.hive=DEBUG` |
| **Solr (Audits)** | `/var/solr/logs/solr.log` | `INFO` / `WARN` |

---

## 1. Issue: Ranger Policy Not Applied

### Symptoms
* You updated a policy in Ranger Admin UI, but users still experience `Permission Denied` or can access resources they shouldn't.

### Root Cause Analysis
1. **Policy Poll Interval Delay**: The Ranger plugin polls the Admin server periodically (default: 30 seconds). The update might not have synced yet.
2. **Plugin Connection Failure**: The plugin cannot connect to Ranger Admin REST endpoints due to network routes or SSL certificate trust.
3. **Local Cache Stale**: The local cache file on the node is corrupted or lacks write permissions for the service daemon.

### Resolution Steps
1. Force policy sync by checking the plugin status in Ranger Admin under the **Audit ➔ Plugins** tab. Look at the last sync time.
2. Manually trigger a policy reload by pulling the REST API payload on the affected node:
   ```bash
   curl -i -u admin:RangerAdminPassword123 http://ranger-admin:6080/service/plugins/policies/download/production_hive
   ```
3. Inspect the cache folder permissions. For HDFS:
   ```bash
   ls -la /etc/ranger/production_hdfs/policycache/
   # Ensure it is owned by 'hdfs' user
   chown -R hdfs:hadoop /etc/ranger/production_hdfs/policycache/
   ```

---

## 2. Issue: Ranger Plugin Sync Failure

### Symptoms
* Error in plugin logs: `Failed to download policies. Error: 401 Unauthorized` or `ConnectException: Connection refused`.

### Resolution Steps
1. Verify the `ranger.plugin.[service].policy.rest.url` in `ranger-[service]-security.xml` points to the correct Ranger Admin host and port.
2. Check credential verification. If using SSL, ensure that the Java Truststore (`cacerts` or a custom truststore defined in `ranger-policymgr-ssl.xml`) contains the Ranger Admin certificate.
3. Validate user rights. The Ranger Admin user used by the plugin must have policy download permissions.

---

## 3. Issue: Kerberos Ticket Expiry and Keytab Failures

### Symptoms
* Log traces: `GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos credential)` or `KrbException: Ticket expired`.

### Resolution Steps
1. Check that the Kerberos ticket-granting ticket (TGT) is still valid:
   ```bash
   klist -kt /etc/security/keytabs/hive.service.keytab
   ```
2. Manually renew the ticket using the keytab principal:
   ```bash
   kinit -kt /etc/security/keytabs/hive.service.keytab hive/hive-server2.enterprise.com@ENTERPRISE.COM
   ```
3. Ensure the service wrapper script auto-renews tickets. In Java, configure JAAS configurations properly:
   ```properties
   useKeyTab=true
   storeKey=true
   refreshKrb5Config=true
   ```

---

## 4. Issue: Atlas Lineage Metadata Missing

### Symptoms
* A Hive query executes successfully, but no new entity or lineage diagram appears in the Atlas Web UI.

### Root Cause Analysis
1. **Hive Hook Classpath Issue**: The Atlas Hive hook JAR files are missing from Hive's auxiliary classpaths.
2. **Kafka Broker Unreachable**: The Hive Hook runs asynchronously inside the Hive JVM. It tries to push a lineage message to Kafka (`ATLAS_HOOK` topic), but Kafka is unavailable or rejecting connections.
3. **Kafka Topic Missing**: The `ATLAS_HOOK` topic was not created, or the partition count is misconfigured.

### Resolution Steps
1. Check if the Atlas Hook is loaded. Open HiveServer2 logs and search for:
   ```
   org.apache.atlas.hive.hook.HiveHook - Inside HiveHook.run()
   ```
2. If the class is not found, copy the Atlas Hook jars to Hive's lib directory:
   ```bash
   cp /opt/apache-atlas/hook/hive/* /opt/hive/lib/
   ```
3. Verify Kafka connections using standard CLI tools:
   ```bash
   kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ATLAS_HOOK --from-beginning --max-messages 5
   ```
4. Verify `atlas-application.properties` is in the Hive classpath. If Hive cannot locate this properties file, it will fail to connect to the metadata notification bus.
