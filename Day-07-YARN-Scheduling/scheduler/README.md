# YARN Scheduler Implementation Notes

This directory serves as a reference point for analyzing, modifying, and building YARN scheduler code from source.

## Code Path Maps
The active scheduler source code resides in the Apache Hadoop repository at:
* **Capacity Scheduler**: `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CapacityScheduler.java`
* **Fair Scheduler**: `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/fair/FairScheduler.java`
* **Dominant Resource Fairness**: `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/resource/DominantResourceCalculator.java`

Refer to the main [README.md](file:///d:/30_Days_of_Modern_Hadoop_Ecosystem/Day-07-YARN-Scheduling/README.md) for compilation and remote debugging instructions.
