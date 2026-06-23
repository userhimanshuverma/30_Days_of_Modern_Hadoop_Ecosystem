# Day 2 References — HDFS Architecture & Write Path Internals

Here is a curated collection of references, papers, official documentation, and technical deep-dives to supplement your learning.

---

## 📄 Academic Foundation Papers

* **The Google File System (GFS)**
  * *Author(s):* Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung (Google, SOSP 2003)
  * *Description:* The original design paper that inspired Apache HDFS. It explains why Google built a distributed filesystem on commodity hardware, introducing the concepts of single-master architectures, large chunk sizes, and client-side pipelined writes.
  * *Link:* [Google GFS Paper PDF](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf)

---

## 📘 Apache Hadoop Official Documentation

* **HDFS Architecture Guide**
  * *Description:* The official architecture overview detailing NameNode/DataNode processes, replication mechanisms, safe mode, and file systems metadata.
  * *Link:* [Apache HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
* **HDFS Commands Guide**
  * *Description:* Official command reference sheet for HDFS file systems command line tools (`hdfs dfs`, `hdfs dfsadmin`, `hdfs fsck`).
  * *Link:* [Apache HDFS Commands Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html)
* **HDFS High Availability Using Active/Standby NameNodes**
  * *Description:* Details the design and setup of high availability using Quorum Journal Manager (QJM) and ZooKeeper failover controllers.
  * *Link:* [Apache HDFS HA QJM Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html)

---

## 🛠️ Source Code Repository

* **Apache Hadoop GitHub Mirror**
  * *NameNode Internals:* [FSNamesystem.java](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java)
  * *DataNode Block Receiver:* [BlockReceiver.java](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java)
  * *DFSClient Write Pipeline:* [DFSOutputStream.java](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/DfsClientConf.java)

---

## 🏢 Enterprise Case Studies & Engineering Blogs

* **Hadoop Platform at Netflix Scale**
  * *Summary:* An insightful review of how Netflix operated exabyte-scale HDFS infrastructure for data pipelines before transitioning to S3/Iceberg architectures.
  * *Link:* [Netflix Tech Blog: Hadoop Platform](https://netflixtechblog.com/)
* **eBay's Multi-Tenant HDFS Scale Operations**
  * *Summary:* Detailed breakdown of eBay's operations managing HDFS storage, namespace limitations, and their custom solutions for small files optimization.
  * *Link:* [eBay Tech Blog](https://tech.ebayinc.com/)
* **Cloudera Engineering: HDFS Rack Awareness**
  * *Summary:* Explains the design principles of rack awareness topologies, layout planning, and configuration settings in enterprise deployments.
  * *Link:* [Cloudera HDFS Rack Awareness Deep-Dive](https://blog.cloudera.com/)
