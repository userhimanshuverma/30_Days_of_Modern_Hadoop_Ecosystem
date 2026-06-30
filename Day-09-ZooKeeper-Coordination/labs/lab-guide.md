# Lab Guide: Deploying and Exploring a 3-Node ZooKeeper Cluster

This guide walks you through deploying, verifying, and testing a 3-node Apache ZooKeeper cluster locally using Docker. You will interact with client nodes, test distributed consensus, observe watch triggers, and run failure recovery scenarios.

---

## 🛠️ Step 1 — Prerequisites & Directory Layout

Before starting, ensure you have:
1. **Docker Desktop** installed and running.
2. **Docker Compose v2** installed.
3. Bash shell (WSL/Linux/macOS) to execute verification scripts.

Navigate to the project directory:
```bash
cd Day-09-ZooKeeper-Coordination
```

Ensure all verification scripts in `scripts/` are executable:
```bash
chmod +x scripts/*.sh
```

---

## 🚀 Step 2 — Start the ZooKeeper Ensemble

Launch the 3-node ZooKeeper cluster by running the deployment script. This starts the containers and checks their health checks:
```bash
./scripts/start-cluster.sh
```

Expected Output:
```text
=== Starting Apache ZooKeeper 3-Node Ensemble ===
[+] Running 8/8
 ✔ Network zk-net            Created
 ✔ Volume "zk1_data"         Created
 ✔ Volume "zk1_datalog"      Created
 ...
=== Waiting for ZooKeeper instances to pass healthchecks... ===
Checking zookeeper1 health... healthy
Checking zookeeper2 health... healthy
Checking zookeeper3 health... healthy

[SUCCESS] All ZooKeeper nodes are healthy and running!
```

---

## 🔍 Step 3 — Verify Quorum Consensus & Mode

Now that all containers are online, query their internal roles (Leader vs. Follower) and parameters:
```bash
./scripts/verify-quorum.sh
```

This sends the `stat` 4-letter word command to each node via Netcat.
Verify that:
- Exactly **one node** is in `Mode: leader`.
- The other **two nodes** are in `Mode: follower`.
- The transaction ID (`Zxid`) is aligned across all members.

To quickly find the current leader, run:
```bash
./scripts/verify-leader.sh
```

---

## 💻 Step 4 — Perform Client CRUD Operations

Interact directly with the ZooKeeper command-line client (`zkCli.sh`) inside `zookeeper1` to write data, and verify that it propagates to `zookeeper2` and `zookeeper3`.

### 1. Connect to ZooKeeper 1 and create a ZNode
```bash
docker exec -it zookeeper1 zkCli.sh -server localhost:2181
```

Inside the interactive shell:
```text
[zk: localhost:2181(CONNECTED) 0] create /my-first-znode "hadoop-coordination"
Created /my-first-znode

[zk: localhost:2181(CONNECTED) 1] get /my-first-znode
hadoop-coordination
```

### 2. Verify on ZooKeeper 2
Open a separate connection to node 2 and retrieve the node:
```bash
docker exec -it zookeeper2 zkCli.sh -server localhost:2181
```
```text
[zk: localhost:2181(CONNECTED) 0] get /my-first-znode
hadoop-coordination
```
*(Notice that the data was replicated instantly via the Zab consensus protocol!)*

### 3. Exit the interactive CLI
```text
[zk: localhost:2181(CONNECTED) 1] quit
```

Alternatively, you can automate this validation by running:
```bash
./scripts/verify-znode.sh
```

---

## ⏱️ Step 5 — Verify Watch Notification Events

A core feature of ZooKeeper is client notifications (Watches) on path changes. To test this:

Run the automated watch script:
```bash
./scripts/verify-watch.sh
```

This script:
1. Spawns a client in the background on `zookeeper2` with a watch set on `/watch-test-node`.
2. Updates `/watch-test-node` from `zookeeper1`.
3. Verifies that the client logs intercept the `NodeDataChanged` event.

---

## 💥 Step 6 — Simulate Leader Failure & Observe Failover

To test High Availability and the Zab leader election mechanism:

Run the failure script:
```bash
./scripts/simulate-failure.sh
```

### What this script does:
1. Detects which node is the current Leader (e.g., `zookeeper1`).
2. Stops that node (`docker stop zookeeper1`).
3. Queries the remaining two nodes (`zookeeper2`, `zookeeper3`). A new election is triggered, electing one of the survivors as the new leader.
4. Restarts `zookeeper1` and verifies it rejoins the cluster, automatically becoming a follower.

---

## 🧹 Step 7 — Clean Up

Once the exercises are complete, tear down the containers and clean up Docker volumes:
```bash
./scripts/stop-cluster.sh
```
