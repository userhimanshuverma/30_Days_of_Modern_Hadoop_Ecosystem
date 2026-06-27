# Day 6 References & Deep Reads: YARN Architecture

This document compiles foundational research papers, architectural design documents, conference slides, and official guides related to Apache Hadoop YARN (Yet Another Resource Negotiator).

---

## 📖 Seminal Papers & Architecture Specs

### 1. [Apache Hadoop YARN: Yet Another Resource Negotiator (ACM SoCC 2013)](https://dl.acm.org/doi/10.1145/2537816.2537821)
* **Authors**: Arun C. Murthy, Vinod Kumar Vavilapalli, et al.
* **Description**: The core academic paper introducing YARN to the distributed systems community.
* **Key Topics**: JobTracker scalability limits, separation of resource management from execution monitoring, scheduling performance benchmarks.

### 2. [Hadoop Next Generation MapReduce Architecture Design (JIRA MAPREDUCE-279)](https://issues.apache.org/jira/browse/MAPREDUCE-279)
* **Description**: The original design specification ticket outlining why MapReduce needed to be split into a generalized resource negotiator layer (YARN) and a processing engine layer (MapReduce v2).

---

## 📖 Official Apache Documentation

### 3. [Apache Hadoop YARN Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-yarn/hadoop-yarn-site/YARN.html)
* **Description**: The primary entrypoint for the YARN developer guide, detailing the interactions of the ResourceManager, NodeManager, and ApplicationMaster.

### 4. [Capacity Scheduler Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html)
* **Description**: Detailed reference for capacity queues, memory/CPU controls, user limits, queue ACLs, elastic reservations, and preemption parameters.

### 5. [YARN ResourceManager High Availability](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-yarn/hadoop-yarn-site/ResourceManagerHA.html)
* **Description**: Configuration guidelines for setting up Active/Standby ResourceManagers synchronized via ZooKeeper State Store and ZKFC.

---

## 🎓 Engineering Blogs & Real-World Operations

### 6. [Uber Engineering: Scaling YARN Cluster to 10,000+ Nodes](https://www.uber.com/blog/)
* **Description**: Case study on how Uber manages multi-tenant analytical queries across shared compute resources, addressing GC optimizations and queue layouts.

### 7. [Cloudera Blog: Tuning YARN Memory and CPU Allocation](https://blog.cloudera.com/)
* **Description**: Step-by-step sizing guidelines for calculating memory heap sizes, vcore values, and OS resource reservations on physical bare-metal worker nodes.

### 8. [Linkedin Engineering: YARN Preemption at scale](https://engineering.linkedin.com/)
* **Description**: Operational insights on configuring capacity-scheduler preemption without terminating high-priority long-running queries prematurely.
