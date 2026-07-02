# Hands-On Lab: Build a Complete Kafka Producer-Consumer Pipeline

This hands-on lab guides you through compiling, running, and analyzing a production-grade Kafka Producer and Consumer pipeline in a local multi-broker KRaft environment.

## Objectives
- Deploy a 3-broker KRaft cluster with the AKHQ monitoring web console.
- Compile and run an Idempotent Producer generating transactional JSON payloads.
- Run a Consumer Group using the modern `CooperativeStickyAssignor` and manual offset commits.
- Observe partition rebalancing when consumers join or leave dynamically.
- Monitor offset progress and partition consumer lag.

---

## Prerequisites
- **Java JDK 11** or higher installed.
- **Apache Maven** installed.
- **Docker & Docker Compose** installed and running.
- Unix bash terminal (or Git Bash on Windows).

---

## Step 1: Spin Up the Cluster
From the root of `Day-11-Kafka-Producers-Consumers`, spin up the multi-broker cluster in detached mode:

```bash
docker-compose -f docker/docker-compose.yml up -d
```

Verify that all containers are healthy:
```bash
docker-compose -f docker/docker-compose.yml ps
```
*Note: The brokers use custom health checks using `kafka-topics.sh` to ensure they are fully initialized before client applications connect.*

---

## Step 2: Build the Java Applications
Navigate to the `labs/` directory containing the Maven project and compile it:

```bash
cd labs
mvn clean package
```

This compiles `OrderPayload.java`, `OrderProducer.java`, and `OrderConsumer.java`, bundling them into a fat shaded JAR in the `target/` directory:
- `target/day-11-kafka-clients-1.0-SNAPSHOT.jar`

---

## Step 3: Run the Verification Scripts

### 1. Create the `orders` topic and verify:
Run the producer verification script to initialize the topic:
```bash
../scripts/verify-producer.sh
```
Choose option **3** (Skip interactive test) just to let the script create the `orders` topic with 3 partitions and a replication factor of 3.

To verify partitions are created, run:
```bash
../scripts/verify-offsets.sh
```
You should see:
```
Partition | Log Start Offset | Log End Offset | Message Count
    0     |      0           |     0          | 0
    1     |      0           |     0          | 0
    2     |      0           |     0          | 0
```

---

## Step 4: Run the Consumer Service
Open a new terminal, navigate to the `Day-11-Kafka-Producers-Consumers/scripts` directory, and run the consumer verification script:

```bash
./verify-consumer.sh
```
Choose option **2** to start the Java-based `OrderConsumer` utilizing `configs/consumer.properties`.

You will see:
```
REBALANCE COMPLETE: Partitions assigned to this consumer: [orders-0, orders-1, orders-2]
Subscribed to topic: orders. Beginning poll loop...
```
Since this is the only consumer in the `order-processing-group` consumer group, it is assigned all 3 partitions.

---

## Step 5: Start the Producer Service
Open another terminal, navigate to `Day-11-Kafka-Producers-Consumers/scripts`, and run:

```bash
./verify-producer.sh
```
Choose option **2** to run the Java-based `OrderProducer` using `configs/producer.properties`.

The producer will start sending 100 transaction records, one every 500ms. Watch the log output:
```
Delivered payload. Key: cust_4 -> Partition: 0 | Offset: 0 | Timestamp: 1720000000000
Delivered payload. Key: cust_1 -> Partition: 2 | Offset: 0 | Timestamp: 1720000000500
```
Notice how records with the same customer ID (e.g. `cust_4`) are routing to the same partition.

In the consumer terminal, you will see logs showing that messages are fetched, processed, and manually committed:
```
Fetched 10 records in this poll batch.
PROCESSED record - Key: cust_4 | Partition: 0 | Offset: 0 | Payload: OrderPayload{orderId='...', customerId='cust_4', amount=...}
Initiating synchronous manual commit of offsets: {orders-0=OffsetAndMetadata{offset=1, metadata='Metadata: Processed order ...'}}
Manual commit succeeded.
```

---

## Step 6: Scale the Consumer Group (Observe Cooperative Rebalancing)
With the producer still running, open a third terminal and start another instance of the consumer:

```bash
cd Day-11-Kafka-Producers-Consumers/scripts
./verify-consumer.sh
```
Select option **2** to start a second Java consumer.

In a fourth terminal, monitor the consumer group state changes:
```bash
./verify-rebalancing.sh
```

### What Happens Internally:
1. The new consumer issues a JoinGroup request to the Group Coordinator broker.
2. The group state transitions from `Stable` -> `CompletingRebalance`.
3. Under the cooperative sticky protocol, only one partition is revoked from the first consumer and assigned to the second consumer, avoiding a full stop-the-world block of all partitions.
4. Watch the consumer logs:
   - **Consumer 1 logs**: `REBALANCE TRIGGERED: Revoking partitions... [orders-2]` (commits offsets first!)
   - **Consumer 2 logs**: `REBALANCE COMPLETE: Partitions assigned to this consumer: [orders-2]`
   - Both consumers will now divide the incoming partition workload dynamically.

Check active group member assignments:
```bash
./verify-consumer-group.sh
```

---

## Step 7: Simulate Consumer Failure
Simulate a production node crash by forcing the second consumer to quit (press `CTRL+C` or kill the process).
Observe the rebalance monitor logs:
- The Group Coordinator waits for the consumer heartbeat timeout (`session.timeout.ms=45000` or 45 seconds).
- Once expired, the coordinator marks the consumer dead, transitions the group to rebalance, and reassigns partition `orders-2` back to Consumer 1.
- During this window, Consumer 1 resumes processing for partition `orders-2` smoothly from the last manually committed offset.

---

## Step 8: Visualizing Lag in AKHQ UI
Open your web browser and navigate to `http://localhost:8080`.
- Under **Topics**, click on `orders` to see partition statistics, message rates, and configurations.
- Under **Consumer Groups**, inspect `order-processing-group` to see partition assignments, client host IPs, current offset values, and consumer lag.

---

## Step 9: Clean Up
Once complete, stop the cluster and remove associated volumes:

```bash
docker-compose -f ../docker/docker-compose.yml down -v
```
This stops all brokers, the UI, and clears local data folders.
