# Day 5 Lab Guide: HDFS Performance Benchmarking and Optimization

This lab guide will walk you through setting up a local containerized 3-node HDFS cluster, running performance benchmarks to isolate bottlenecks, and applying production tuning configurations.

---

## Prerequisites

1. **Docker & Docker Compose** installed on your host machine.
2. **CPU/RAM Allocations**: At least 4 CPU cores and 6 GB of RAM assigned to Docker.
3. Access to a terminal/Powershell to execute commands.

---

## Step 1: Cluster Provisioning

First, navigate to the docker directory and start the HDFS cluster services:

```bash
cd Day-05-HDFS-Performance-Tuning/docker
docker-compose up -d --build
```

Verify that all containers (NameNode, 3x DataNodes, Client, Prometheus, and Grafana) are running successfully:

```bash
docker-compose ps
```

*Expected Output:*
```text
NAME                                       COMMAND                  SERVICE             STATUS              PORTS
docker-client-1                            "/bootstrap-hadoop.s…"   client              running             
docker-datanode1-1                         "/bootstrap-hadoop.s…"   datanode1           running             0.0.0.0:9864->9864/tcp
docker-datanode2-1                         "/bootstrap-hadoop.s…"   datanode2           running             0.0.0.0:9865->9864/tcp
docker-datanode3-1                         "/bootstrap-hadoop.s…"   datanode3           running             0.0.0.0:9866->9864/tcp
docker-grafana-1                           "/run.sh"                grafana             running             0.0.0.0:3000->3000/tcp
docker-namenode-1                          "/bootstrap-hadoop.s…"   namenode            running             0.0.0.0:9870->9870/tcp, 0.0.0.0:9904->9904/tcp
docker-prometheus-1                        "/bin/prometheus --c…"   prometheus          running             0.0.0.0:9090->9090/tcp
```

---

## Step 2: Verification of Cluster and Rack Awareness

Exec into the client container to verify the topology and JMX connectivity:

```bash
docker-compose exec client bash
```

Once inside the client container, run the DataNode verification script:

```bash
/tmp/scripts/verify-datanodes.sh
```

Observe the output to check that the nodes are distributed across racks `/rack1` and `/rack2` as defined by the rack topology script.

---

## Step 3: Run the Throughput Benchmark

Run the HDFS master benchmark script which automates throughput checks, block size comparisons, and the small files problem simulator:

```bash
/tmp/scripts/benchmark-hdfs.sh
```

### Analysis of the Benchmarks:
1. **TestDFSIO (Throughput)**: Note the write rate vs the read rate. Usually, reads are faster due to caching.
2. **Block Size Comparison**: Observe the execution time differences between uploading the 512MB payload with 32MB, 128MB, and 256MB block sizes. 
   - *Why does this occur?* With 32MB blocks, the client has to request block allocations from the NameNode 16 times per file, incurring RPC overhead. With 256MB blocks, it only requests 2 allocations.
3. **Small Files Test**: Notice that writing 2,000 files of 10KB sequentially (totaling ~20MB of payload) takes significantly longer than uploading a single 20MB contiguous file.
   - *Why?* For each file, the client must negotiate metadata with the NameNode, establish write pipelines, and close streams, resulting in severe processing latency.

---

## Step 4: Run HDFS Balancer

To test balance operations, we can set the balancer bandwidth limit and run the dry check:

```bash
/tmp/scripts/verify-balancer.sh
```

Verify that the bandwidth limit update is acknowledged by the cluster:

```text
Balancing bandwith is 104857600
```

---

## Step 5: Monitor Cluster Metrics

1. Open a web browser on your host machine.
2. Navigate to Grafana at: **`http://localhost:3000`**
3. Log in using default credentials:
   - **Username**: `admin`
   - **Password**: `admin`
4. Access the **HDFS Performance Dashboard** provisioned in Grafana.
5. Review the real-time plots showing NameNode RPC times, live DataNodes, HDFS capacities, and JVM Heap usage while running the benchmarks.

---

## Step 6: Cleanup

To tear down the cluster environment and purge volumes, exit from the client container and execute:

```bash
docker-compose down -v
```

This completes the hands-on lab.
