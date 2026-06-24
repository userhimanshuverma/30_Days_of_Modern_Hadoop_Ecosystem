# Day 3 References & Deep Reads: HDFS High Availability

This document contains curated academic papers, architectural design notes, conference presentations, and official documentation related to HDFS High Availability (HA), Quorum Journal Manager (QJM), and ZooKeeper Failover Controllers (ZKFC).

---

## 📖 Official Documentation & Design Papers

### 1. [Apache Hadoop HDFS High Availability with QJM](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html)
* **Description**: The official user guide detailing configuration, fencing, architecture, and deployment options using the Quorum Journal Manager.
* **Key Topics**: Logical Nameservices, automatic failover setup, command-line operations (`hdfs haadmin`).

### 2. [HDFS High Availability Design Specification (JIRA HDFS-1623)](https://issues.apache.org/jira/browse/HDFS-1623)
* **Description**: The core JIRA ticket and attached design document detailing the transition of HDFS from a Single Point of Failure (SPOF) to a High Availability model.
* **Key Topics**: Standby state design, synchronization logic, checkpointing delegation from Standby to Active.

### 3. [Quorum Journal Manager (QJM) Design Spec (JIRA HDFS-3077)](https://issues.apache.org/jira/browse/HDFS-3077)
* **Description**: Detailed architectural proposal and design decisions for replacing NFS shared edits directories with a dedicated Paxos-like JournalNode quorum.
* **Key Topics**: Quorum consensus logic, transaction ID sequencing, epoch-based fencing.

---

## 🎓 Academic Papers & Whitepapers

### 4. [The Hadoop Distributed File System (Shvachko et al.)](https://www.computer.org/csdl/proceedings-article/msst/2010/3941a001/12OmNvcaJ52)
* **Description**: The foundational paper detailing the original design of HDFS and the NameNode single point of failure constraints.
* **Key Topics**: Block mapping, namespace management, original backup node limitations.

### 5. [ZooKeeper: Wait-free coordination for ZooKeeper-like services (Hunt et al.)](https://www.usenix.org/conference/usenix-atc-10/zookeeper-wait-free-coordination-internet-scale-systems)
* **Description**: The seminal USENIX paper describing ZooKeeper's Zab protocol and hierarchical lock mechanisms used by ZKFC.
* **Key Topics**: Hierarchical namespace, watchers, ephemeral nodes, consensus mechanics.

---

## 🎥 Conference Talks & Engineering Blogs

### 6. [Netflix TechBlog: Operating Hadoop at Netflix Scale](https://netflixtechblog.com/)
* **Description**: Case study on how Netflix monitors, scales, and manages HDFS metadata performance, including handling large namespaces in HA configurations.
* **Key Topics**: JVM garbage collection tuning for NameNodes, hardware layouts for JournalNodes.

### 7. [Hortonworks/Cloudera: Architecting and Deploying Hadoop HA](https://blog.cloudera.com/)
* **Description**: Best practices for multi-rack physical node topologies, designing robust fencing scripts, and configuring hardware watchdogs.
* **Key Topics**: SSH fencing, IPMI/PDU power fencing, network routing isolation.
