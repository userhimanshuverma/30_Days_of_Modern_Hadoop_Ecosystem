# Day 5 References: HDFS Performance Optimization

This list contains high-quality documentation, engineering blog posts, white papers, and source code references for further reading on HDFS tuning.

---

## 📖 Official Documentation

1. **Apache Hadoop HDFS Architecture Guide**
   * Detailed overview of the HDFS design goals, metadata checkpoints, and replication pipelines.
   * [https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)

2. **HDFS Commands Reference**
   * Official CLI guide for managing balancer settings, running fsck, and using dfsadmin options.
   * [https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html)

3. **Short-Circuit Local Reads Configuration**
   * Setup guides for domain sockets and security protocols.
   * [https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)

---

## 🔬 Engineering Blogs & Case Studies

1. **Netflix Tech Blog: Scaling HDFS to Petabytes**
   * Deep-dive into Netflix S3-HDFS hybrid models, managing block sizes, and balancing large clusters.
   * [https://netflixtechblog.com/](https://netflixtechblog.com/)

2. **Cloudera Blog: Tuning HDFS for Performance**
   * Excellent architectural breakdowns of DataNode transceiver queues, NameNode heap sizing, and short-circuit reads.
   * [https://blog.cloudera.com/](https://blog.cloudera.com/)

3. **Hadoop Java GC Sizing (Uber / Linkedin Engineering)**
   * Whitepapers and blog posts detailing G1GC heap profiles, humongous allocations, and tuning pause times on 100GB+ heaps.
   * [https://engineering.linkedin.com/blog](https://engineering.linkedin.com/blog)

---

## 💻 Source Code References

1. **`BlockReaderLocal.java`**
   * HDFS Client implementation for reading local block files directly from the OS page cache via file descriptors.
   * [Apache Hadoop Github - BlockReaderLocal](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/impl/BlockReaderLocal.java)

2. **`Balancer.java`**
   * Source code for pairing over-utilized and under-utilized DataNodes, executing moves, and bandwidth throttling logic.
   * [Apache Hadoop Github - Balancer](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/Balancer.java)
