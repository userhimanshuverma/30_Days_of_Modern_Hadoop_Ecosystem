# References and Deep Reads — Apache ZooKeeper

This document compiles core theoretical papers, official documentation, source links, and engineering articles regarding Apache ZooKeeper, distributed coordination, and consensus algorithms.

---

## 📜 Academic Papers & Specifications

1. **ZooKeeper: Wait-free coordination for Internet-scale systems** (USENIX ATC 2010)
   - *Authors:* Patrick Hunt, Mahadev Konar, Flavio P. Junqueira, Benjamin Reed.
   - *Description:* The foundational research paper introducing ZooKeeper's wait-free design, hierarchical namespace, and event notifications.
   - *Link:* [https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf](https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf)

2. **Zab: High-performance broadcast for primary-backup systems** (DSN 2011)
   - *Authors:* Flavio P. Junqueira, Benjamin Reed, Marco Serafini.
   - *Description:* The formal specification of the Zab (ZooKeeper Atomic Broadcast) protocol, detailing leader election, recovery phase, and broadcast phase.
   - *Link:* [https://ieeexplore.ieee.org/document/5958223](https://ieeexplore.ieee.org/document/5958223)

---

## 📖 Official Apache ZooKeeper Documentation

1. **ZooKeeper Overview & Getting Started**
   - Comprehensive introduction to ZK coordination concepts, installation, and deployment.
   - *Link:* [https://zookeeper.apache.org/doc/current/zookeeperOver.html](https://zookeeper.apache.org/doc/current/zookeeperOver.html)

2. **ZooKeeper Administrator's Guide**
   - Detailed deployment setups, configuration properties (`zoo.cfg`), JVM tuning, purging logs, monitoring metrics, and security (TLS/SASL).
   - *Link:* [https://zookeeper.apache.org/doc/current/zookeeperAdmin.html](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html)

3. **ZooKeeper Programmer's Guide**
   - API guide for managing connections, creating znodes (persistent, ephemeral, sequential), handle sessions, and register watches.
   - *Link:* [https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

4. **ZooKeeper recipes and Solutions**
   - Guide on implementing common distributed design patterns (locks, queues, barriers, elections) using ZooKeeper APIs.
   - *Link:* [https://zookeeper.apache.org/doc/current/zookeeperRecipes.html](https://zookeeper.apache.org/doc/current/zookeeperRecipes.html)

---

## 💻 Source Code & Client Libraries

1. **Apache ZooKeeper Official GitHub Repository**
   - Contains the core Java engine, C client library, and build tools.
   - *Link:* [https://github.com/apache/zookeeper](https://github.com/apache/zookeeper)

2. **Apache Curator (Java Client)**
   - The high-level Java client library providing built-in recipes (Distributed Lock, Leader Latch, Service Cache).
   - *Link:* [https://curator.apache.org/](https://curator.apache.org/)

3. **Kazoo (Python Client)**
   - Python client library implementing ZooKeeper APIs, watchers, and common coordination patterns.
   - *Link:* [https://kazoo.readthedocs.io/](https://kazoo.readthedocs.io/)

---

## 📰 Engineering Blogs & Case Studies

1. **Confluent: ZooKeeper's Role in Apache Kafka**
   - Details how Kafka broker discovery, controller election, and topic metadata were managed via ZooKeeper, and the path to KIP-500 (removing ZooKeeper dependency with KRaft).
   - *Link:* [https://www.confluent.io/blog/kafka-without-zookeeper-a-sneak-peek/](https://www.confluent.io/blog/kafka-without-zookeeper-a-sneak-peek/)

2. **LinkedIn Engineering: Scaling ZooKeeper in Production**
   - Challenges and architectural optimizations LinkedIn implemented to scale ZooKeeper ensembles coordinating large-scale Kafka and search infrastructures.
   - *Link:* [https://engineering.linkedin.com/blog/2016/10/scaling-zookeeper-in-production](https://engineering.linkedin.com/blog/2016/10/scaling-zookeeper-in-production)

3. **Hadoop Community: HDFS High Availability (HA) via ZooKeeper**
   - Explains the internals of active-standby failover coordination using ZKFC (ZooKeeper Failover Controller) and active locks.
   - *Link:* [https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html#Using_ZooKeeper_for_automatic_failover](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html#Using_ZooKeeper_for_automatic_failover)
