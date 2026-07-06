# Day 15: Apache Tez Production Troubleshooting Playbook

This playbook outlines common issues, root causes, diagnostic strategies, and resolution steps for Apache Tez execution engine in production Hadoop and Hive clusters.

---

## 🚨 1. Out of Memory Errors (OOM) in Vertex Tasks

### Symptom
- Tez job fails with `java.lang.OutOfMemoryError: Java heap space` or `GC overhead limit exceeded`.
- Containers are killed by the YARN NodeManager for exceeding physical memory limits.
- Container log contains:
  ```text
  FATAL [TezChild] org.apache.tez.runtime.task.TezChild: Error running child task
  java.lang.OutOfMemoryError: Java heap space
  ```

### Root Cause
1. **Misconfigured Heap to Container Sized Ratio**: The JVM heap size (`-Xmx`) allocated to Tez tasks exceeds or is too close to the container's physical limit defined by YARN.
2. **Sort Buffer is Too Large**: `tez.runtime.io.sort.mb` is set too high relative to the JVM heap, leaving insufficient space for execution processing memory.
3. **Data Skew**: A particular key has an excessively high number of values, concentrating all record handling within a single reducer/vertex task.

### Resolution
1. **Enforce standard heap-to-container ratio**: Ensure that `-Xmx` is set to **75-80%** of `tez.task.resource.memory.mb`. 
   - Example: If `tez.task.resource.memory.mb = 2048`, set `-Xmx` to `1600m` in `tez.container.max.java.opts`.
2. **Tune Sort Buffers**: Set `tez.runtime.io.sort.mb` to no more than **40%** of the JVM heap size.
3. **Address Data Skew**:
   - In Hive, enable skew optimizations:
     ```sql
     SET hive.optimize.skewjoin=true;
     SET hive.skewjoin.key=100000;
     ```

---

## ⏳ 2. Container Allocation Timeout (AM Hung State)

### Symptom
- Tez job hangs at `Submitting DAG` or `Status: RUNNING` with 0% progress across all vertices.
- YARN Resource Manager shows the ApplicationMaster is in `RUNNING` state but no tasks are active.
- Logs from the ApplicationMaster report:
  ```text
  [WARN] [AMContainerAllocator] YARN Resource allocation timeout. Requesting...
  [INFO] Container request list is empty. Waiting for containers...
  ```

### Root Cause
1. **Resource Starvation**: YARN queues are exhausted, and there are not enough available resources (memory or vcores) to start the first Vertex container.
2. **Container Sizing Exceeds Limits**: The requested Tez task container size (`tez.task.resource.memory.mb` or `hive.tez.container.size`) exceeds the maximum YARN container allocation limit (`yarn.scheduler.maximum-allocation-mb`).

### Resolution
1. **Verify YARN Capacities**:
   Run the following CLI command to check available cluster capacity:
   ```bash
   yarn queue -status default
   ```
2. **Verify NodeManager Status**:
   Ensure NodeManagers have registered resource capacities:
   ```bash
   yarn node -list
   ```
3. **Reduce Task Container Request**:
   Align the container size requested by Tez with the maximum YARN capacity:
   ```xml
   <!-- Ensure this is less than yarn.scheduler.maximum-allocation-mb -->
   <property>
     <name>tez.task.resource.memory.mb</name>
     <value>1024</value>
   </property>
   ```

---

## 🧬 3. Classloader or Dependency Conflicts (NoSuchMethodError / ClassNotFoundException)

### Symptom
- Job fails immediately on launch with:
  ```text
  java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(...)
  ```
- Or a `java.lang.NoClassDefFoundError: org/apache/tez/...` in Hive execution logs.

### Root Cause
1. **Guava Compatibility Issues**: Hadoop, Hive, and Tez bundle different versions of Google's Guava library. The JVM loads the wrong version first depending on classpath precedence.
2. **Classpath Precedence Misconfiguration**: Classpath settings in YARN or MapReduce are masking Tez dependencies.

### Resolution
1. **Delete Outdated Jars in Hive**:
   For Hive 3.x, remove its default Guava 19.x jar and replace it with Hadoop's Guava 27.x jar:
   ```bash
   rm /opt/hive/lib/guava-19.0.jar
   cp /opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar /opt/hive/lib/
   ```
2. **Enable Classpath Precedence Settings**:
   In `hive-site.xml`, enable user classpath isolation:
   ```xml
   <property>
     <name>hive.classpath.use.jdk.classpath</name>
     <value>true</value>
   </property>
   ```
   In `yarn-site.xml`, ensure whitelist environment variables are correct:
   ```xml
   <property>
     <name>yarn.nodemanager.env-whitelist</name>
     <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,TEZ_CONF_DIR,TEZ_JARS</value>
   </property>
   ```

---

## 🐌 4. Performance Degradation (Slow Job Execution)

### Symptom
- Hive queries execute significantly slower than expected.
- Task starts have significant delays (3-5 seconds between tasks).

### Root Cause
1. **Container Reuse is Disabled**: Tez creates a fresh JVM for every single task, leading to high startup and registration latencies on YARN.
2. **Tez Tarball is Not Cached**: Tez jars are downloaded from HDFS for every task instead of using YARN's Localizer cache.

### Resolution
1. **Enable Container Reuse**:
   In `tez-site.xml`, configure Tez to retain JVMs for subsequent tasks:
   ```xml
   <property>
     <name>tez.am.container.reuse.enabled</name>
     <value>true</value>
   </property>
   <property>
     <name>tez.am.container.reuse.max-holding-time-ms</name>
     <value>10000</value>
   </property>
   ```
2. **Leverage HDFS Distributed Cache for Tez Jars**:
   Store the Tez tarball in HDFS and point `tez.lib.uris` to it. This enables YARN NodeManagers to cache the jars locally on the filesystem:
   ```xml
   <property>
     <name>tez.lib.uris</name>
     <value>hdfs://namenode:9000/apps/tez/tez-0.10.2.tar.gz</value>
   </property>
   ```

---

## 🛠️ 5. Production Debugging Command Sheet

### Get Tez Application Logs
To pull application logs directly from YARN:
```bash
yarn logs -applicationId <application_id>
```

### Inspect active DAG details
To view stats of current Tez DAG runs if the history logging server is running:
```bash
tez-history -dag <dag_id>
```

### Diagnose YARN Queue allocations
To verify which queue is stalling Tez:
```bash
yarn queue -status <queue_name>
```
