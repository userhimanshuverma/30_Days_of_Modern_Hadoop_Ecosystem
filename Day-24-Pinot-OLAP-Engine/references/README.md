# References & Deep Reads: Apache Pinot

This document aggregates official resources, research papers, engineering blogs, and community talks to serve as a library for deep research into real-time OLAP.

---

## 📖 Official Resources
* **Official Website**: [https://pinot.apache.org/](https://pinot.apache.org/)
* **Apache Pinot Documentation**: [https://docs.pinot.apache.org/](https://docs.pinot.apache.org/)
* **GitHub Repository**: [https://github.com/apache/pinot](https://github.com/apache/pinot)
* **Slack Community**: [https://communityinviter.com/apps/apache-pinot/apache-pinot](https://communityinviter.com/apps/apache-pinot/apache-pinot)

---

## 📄 Academic & White Papers
* **Pinot: Realtime OLAP Data Store** (SIGMOD 2018)  
  * *Summary*: The foundational paper describing the architecture of Pinot, its indexes, query execution engine, and LinkedIn's production scale.  
  * *Link*: [Download PDF via ACM](https://dl.acm.org/doi/10.1145/3183713.3190662)
* **Star-Tree Indexing in Pinot**  
  * *Summary*: Comprehensive detail on Pinot's unique star-tree pre-aggregation index and its resolution of multi-dimensional group-by latency.  
  * *Link*: [Apache Pinot Star-tree Documentation](https://docs.pinot.apache.org/operators/indexes/star-tree-index-pre-aggregation)

---

## ⚡ LinkedIn Engineering Blogs (Origins & Use Cases)
* **Introducing Pinot: LinkedIn’s Real-time Analytics Engine** (2014)  
  * *Read for*: Understanding the "Why" behind Pinot's inception at LinkedIn to replace slow batch systems.  
  * *Link*: [LinkedIn Engineering Blog](https://engineering.linkedin.com/playbook/introducing-pinot-linkedins-real-time-analytics-engine)
* **Using Apache Pinot to Serve Who Viewed My Profile**  
  * *Read for*: Detailed analysis of how LinkedIn scales member analytics to millions of active users with strict sub-100ms response requirements.  
  * *Link*: [LinkedIn Engineering Blog Archive](https://engineering.linkedin.com/blog/2020/using-apache-pinot-to-serve-who-viewed-my-profile)
* **Ad Analytics at Scale with Apache Pinot**  
  * *Read for*: Designing real-time advertiser dashboards with high query concurrency.  
  * *Link*: [LinkedIn Ads Engine Blog](https://engineering.linkedin.com/blog/2021/ads-analytics-scale-pinot)

---

## 🎥 Conference & Community Talks
* **Real-time Analytics at Scale with Apache Pinot** (Strata Data Conference)  
  * *Speaker*: Pinot Co-founders.  
  * *Topics*: Architectural walkthrough, Kafka integration, and indexing comparisons.
* **Building a User-Facing Analytics Platform at Uber using Apache Pinot**  
  * *Speaker*: Uber Engineering Team.  
  * *Topics*: Uber's migration from Elasticsearch to Pinot, resulting in a 10x hardware efficiency improvement.  
  * *Link*: [Uber Engineering Blog: Pinot at Uber](https://www.uber.com/en-IN/blog/real-time-analytics-pinot/)
