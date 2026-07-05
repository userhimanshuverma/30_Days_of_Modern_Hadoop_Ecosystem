# References: MapReduce Internals

A curated selection of core papers, books, online tutorials, and enterprise engineering blogs explaining MapReduce internals, optimizations, and architectures.

---

## 📄 Fundamental Research Papers

1. **Google MapReduce Paper**:
   - Title: *MapReduce: Simplified Data Processing on Large Clusters* (2004)
   - Authors: Jeffrey Dean and Sanjay Ghemawat
   - Link: [Google Research Publication](https://research.google/pubs/pub62/)
   - *Significance*: The foundational paper that inspired Apache Hadoop MapReduce and shaped modern distributed processing architectures.

2. **Google GFS Paper**:
   - Title: *The Google File System* (2003)
   - Authors: Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung
   - Link: [Google Research Publication](https://research.google/pubs/pub51/)
   - *Significance*: Outlines the co-location of compute and storage (data locality) that MapReduce depends on.

---

## 📖 Official Documentation

- **Apache Hadoop MapReduce Tutorial**:
  - Link: [Apache Hadoop Documentation](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
  - Details: API references, drivers, map-side sorting configurations, and reducer shuffle setup details.
- **Hadoop MapReduce Next Generation (YARN) Architecture**:
  - Link: [Apache YARN documentation](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)
  - Details: Explains how ResourceManager, NodeManager, and ApplicationMaster manage execution tasks.

---

## 🏛️ Enterprise Engineering Blogs

- **Yahoo! Engineering**:
  - Context: Yahoo was the primary contributor to early MapReduce and YARN scaling projects.
  - Recommended Read: *Hadoop Sorts a Petabyte in 62 Seconds* (Early Hadoop milestones).
- **Cloudera Engineering Blog**:
  - Focus: Performance tuning configurations, JVM memory sizing, G1GC tuning inside yarn containers.
  - Link: [Cloudera Blog - Tuning MapReduce Memory](https://blog.cloudera.com/)
- **LinkedIn Engineering Blog**:
  - Focus: Data skew solutions and telemetry collection of YARN map/reduce metrics.
- **Facebook Engineering (Early Archives)**:
  - Context: Detailed explanations of how Facebook scaled MapReduce for their data warehouses and optimized shuffle bottlenecks.
