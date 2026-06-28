# Day 7 References & Deep Reads

Here are the official documentation links, source codes, blogs, and papers related to YARN Resource Schedulers:

## 🌐 Official Apache Hadoop Documentation
* [Hadoop YARN Capacity Scheduler Guide](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html)
* [Hadoop YARN Fair Scheduler Guide](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/FairScheduler.html)
* [Hadoop Node Labels Administration Guide](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeLabel.html)

## 💻 Source Code References
* [CapacityScheduler.java (Apache GitHub Mirror)](https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java)
* [FairScheduler.java (Apache GitHub Mirror)](https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairScheduler.java)
* [DominantResourceCalculator.java](https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/resource/DominantResourceCalculator.java)

## 📑 Research & Engineering Papers
* *Dominant Resource Fairness: Fair Allocation of Multiple Resource Types* (Ghodsi et al., NSDI 2011) — The foundation of YARN's DRC. [Read Paper PDF](https://www.usenix.org/conference/nsdi11/dominant-resource-fairness-fair-allocation-multiple-resource-types)
* Cloudera Engineering Blog: *Tuning YARN Capacity Scheduler for Production Multi-Tenancy*.
