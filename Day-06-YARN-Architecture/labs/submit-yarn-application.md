# Hands-On Lab: Submit and Monitor YARN Applications

## 📌 Objectives
* Spin up a local Hadoop HDFS + YARN cluster via Docker Compose.
* Verify the health and active resource metrics of the YARN ResourceManager and NodeManager.
* Submit a MapReduce Pi estimation application and track resource allocation metrics.
* Submit a Spark application (Pi calculation) to YARN and explore Spark-on-YARN configuration models.
* Monitor resource limits, containers, and queue status in the ResourceManager Web UI.
* Retrieve aggregated container logs via the YARN CLI.
* Clean up cluster resources safely.

---

## 🛠️ Prerequisites
- Docker and Docker Compose installed and running on the host system.
- Basic familiarity with terminal commands.
- 4GB of free system memory (allocated to Docker engine).

---

## 📦 Step 1: Start the Local YARN Cluster

1. Navigate to the docker directory:
   ```bash
   cd Day-06-YARN-Architecture/docker
   ```

2. Start all services in the background:
   ```bash
   docker compose up -d
   ```
   *Expected Output:*
   ```text
   Creating network "day06-network" with driver "bridge"
   Creating volume "day06_namenode_data_day06" with default driver
   Creating volume "day06_datanode_data_day06" with default driver
   Creating volume "day06_historyserver_data_day06" with default driver
   Creating namenode-day06 ... done
   Creating datanode-day06 ... done
   Creating resourcemanager-day06 ... done
   Creating nodemanager-day06     ... done
   Creating historyserver-day06   ... done
   ```

3. Wait 15-30 seconds for the Java processes to initialize.

---

## 🔍 Step 2: Run Health Diagnostics

Run the pre-configured verification scripts inside the `scripts/` directory to inspect YARN cluster registration states.

1. Verify the **ResourceManager**:
   ```bash
   bash ../scripts/verify-rm.sh
   ```
   *Expected JMX Metrics Output snippet:*
   ```text
   Active NodeManagers:   1
   Lost NodeManagers:     0
   Allocated Memory:      0 MB
   Available Memory:      4096 MB
   ResourceManager HA State: Active
   [SUCCESS] YARN ResourceManager is healthy and active. Connected to 1 active NodeManager(s).
   ```

2. Verify the **NodeManager**:
   ```bash
   bash ../scripts/verify-nm.sh
   ```
   *Expected API Info snippet:*
   ```text
   Node Hostname:         nodemanager-day06
   Total Memory Capacity: 4096 MB
   Total vCores Capacity: 4
   Node Health Status:    true
   [SUCCESS] NodeManager health verification completed successfully. Node is healthy.
   ```

3. Query overall cluster queue metrics:
   ```bash
   bash ../scripts/verify-yarn.sh
   ```
   This prints the registered node list and details about the `default` capacity queue capacity configuration.

---

## 💻 Step 3: Access the Web Interfaces

Verify you can access the admin web interfaces from your host web browser:

* **YARN ResourceManager Web UI**: [http://localhost:8088](http://localhost:8088)
  * Examine the "Cluster Metrics" showing 4GB available RAM and 4 Virtual Cores.
  * Explore the scheduler queue tree (root -> default, production, sandbox) under the "Scheduler" tab.
* **MapReduce JobHistory UI**: [http://localhost:19888](http://localhost:19888)
* **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870)

---

## 🚀 Step 4: Submit a MapReduce Job

Submit the MapReduce `pi` calculation example to YARN to trigger container allocations.

1. Execute the sample job script:
   ```bash
   bash ../scripts/submit-sample-job.sh
   ```
   This script finds the example Jar file inside the ResourceManager container and executes:
   ```bash
   yarn jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar pi 5 10
   ```

2. **While the job is running**, check container allocations:
   * Open a separate terminal and run:
     ```bash
     bash ../scripts/verify-containers.sh
     ```
   * Or refresh the ResourceManager UI at [http://localhost:8088](http://localhost:8088). You will see:
     * One `ApplicationMaster` container launched.
     * Multiple mapper containers running on the `nodemanager-day06`.
     * Memory allocation indicators scale up from `0 MB` to `1536 MB` or `2048 MB`.

3. **Check the Output**:
   When the job completes, the console will print:
   ```text
   Job Finished in X.XX seconds
   Estimated value of Pi is 3.20000000000000000000
   [SUCCESS] MapReduce Job submitted and executed successfully!
   ```

---

## ⚡ Step 5: Submit a Spark Job (Optional Integration)

YARN supports running multi-tenant engines like Apache Spark. Let's submit a Spark job in `yarn-client` mode.

1. Log into the ResourceManager container:
   ```bash
   docker exec -it resourcemanager-day06 bash
   ```

2. YARN BDE images come preloaded with basic spark scripts or packages. Run the spark-submit utility to calculate Pi on YARN (if Spark environment variables are exposed, or run directly):
   ```bash
   # Submit Spark Pi example on YARN cluster
   spark-submit \
     --class org.apache.spark.examples.SparkPi \
     --master yarn \
     --deploy-mode client \
     --driver-memory 512m \
     --executor-memory 512m \
     --executor-cores 1 \
     /examples/jars/spark-examples_*.jar 10
   ```
   *Note: If Spark binaries are not preinstalled in this base image, this demonstrates the standard production syntax used to allocate executor containers via YARN ResourceManager scheduler.*

3. Exit the container:
   ```bash
   exit
   ```

---

## 📋 Step 6: Query Job Logs and History

YARN aggregates container logs into HDFS for centralized storage and debugging.

1. Identify your completed Application ID from the console output or the RM Web UI (e.g., `application_1719545000000_0001`).

2. Query the aggregated logs:
   ```bash
   docker exec -it resourcemanager-day06 yarn logs -applicationId application_1719545000000_0001
   ```
   This retrieves stdout and stderr streams from all Mapper, Reducer, and ApplicationMaster containers that ran during the job.

---

## 🧹 Step 7: Cleanup

Stop the containers and delete the associated volumes to return your local machine to its original state.

1. Navigate to the docker directory:
   ```bash
   cd Day-06-YARN-Architecture/docker
   ```

2. Shut down the cluster and clean volumes:
   ```bash
   docker compose down -v
   ```
