# Day 15: Apache Tez References & Deep Reads

Below is a curated collection of official documentation, research papers, design specifications, and engineering blogs for learning Apache Tez from first principles.

---

## 📖 1. Official Documentation
* [Apache Tez Homepage](https://tez.apache.org/)
* [Apache Tez User Guide](https://tez.apache.org/user_guide.html)
* [Apache Hive on Tez Wiki](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Tez)
* [Apache Hadoop YARN Integration](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)

---

## 🔬 2. Research Papers & Design Documents
* **Tez Design Document**: [Tez: A Platform for Directed Acyclic Graph (DAG) Execution on YARN](https://issues.apache.org/jira/secure/attachment/12569395/Tez-design-doc.pdf)
* **Google's FlumeJava**: *FlumeJava: Easy, Efficient Data-Parallel Pipelines (PLDI 2010)*. This paper heavily influenced Tez's design of logical pipelines and deferred executions.
* **Dryad Paper**: *Dryad: Distributed Data-Parallel Programs from Sequential Building Blocks (EuroSys 2007)*. Dryad laid the groundwork for general DAG-based engines.

---

## 🏭 3. Engineering Blogs & Whitepapers
* **Hortonworks Engineering Blog**: *Tez: Accelerating Hadoop Data Processing* (Legacy Cloudera/Hortonworks archives)
* **Cloudera Engineering Blog**: *Hive on Tez Performance benchmarks at Scale*
* **Apache Contributors**: *Optimizing Tez container reuse strategies on large multi-tenant clusters*
* **LinkedIn Engineering**: *Scaling YARN & Tez for heterogeneous workloads*
