#!/usr/bin/env python3
"""
Day 19: Apache Spark Performance Tuning Lab Code
Location: Day-19-Spark-Performance-Tuning/source/PerformanceTuningLab.py

This script implements hands-on exercises for Spark performance tuning:
1. Skewed Join Performance
2. Repartition vs Coalesce Analysis
3. Caching and Persistence Analysis
4. Adaptive Query Execution (AQE) in Action
5. Shuffle Partitions Tuning
"""

import sys
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when

def get_spark_session(app_name="SparkPerformanceTuningLab"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "hdfs://namenode:9000/shared/spark-logs") \
        .getOrCreate()

def run_lab1_skew(spark):
    """
    Lab 1: Generate skewed dataset & measure Join Performance.
    We force SortMergeJoin by disabling auto broadcast join.
    """
    print("\n========================================================")
    print("LAB 1: Data Skew Join Performance (SortMergeJoin)")
    print("========================================================")
    
    # Disable broadcast joins to force Shuffle Hash Join or SortMergeJoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    
    # 1. Generate Skewed Dataset (e.g. Transaction table where 95% of records have key = 'KEY_SKEW')
    print("Generating skewed transaction dataset (1,000,000 rows)...")
    tx_df = spark.range(0, 1000000) \
        .withColumn("join_key", when(col("id") % 100 < 95, "KEY_SKEW").otherwise(col("id").cast("string"))) \
        .withColumn("amount", rand() * 100)
    
    # 2. Generate Lookup Dataset (e.g. Small dimension table, uniform keys)
    print("Generating lookup dataset (1,000 rows)...")
    lookup_df = spark.range(0, 1000) \
        .withColumn("join_key", when(col("id") == 0, "KEY_SKEW").otherwise(col("id").cast("string"))) \
        .withColumn("category", lit("CAT_A"))
        
    print("\nExecuting join without skew mitigation (AQE SkewJoin Disabled)...")
    start_time = time.time()
    
    # Run the join and trigger an action to execute
    result_df = tx_df.join(lookup_df, "join_key")
    count = result_df.count()
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"Join completed! Result count: {count}")
    print(f"Time Taken (Without Skew Mitigation): {duration:.2f} seconds")
    
    # Show query plan
    print("\nPhysical Execution Plan:")
    result_df.explain()

def run_lab2_partitioning(spark):
    """
    Lab 2: Compare Repartition vs Coalesce.
    """
    print("\n========================================================")
    print("LAB 2: Repartition vs Coalesce Analysis")
    print("========================================================")
    
    # Create a base dataframe
    df = spark.range(0, 500000).withColumn("data", rand())
    print(f"Initial partition count: {df.rdd.getNumPartitions()}")
    
    # Test Coalesce (Shrinking partitions)
    print("\nTesting COALESCE (20 partitions -> 4 partitions)...")
    start_time = time.time()
    # Artificially increase partitions first to have something to shrink
    large_partition_df = df.repartition(20)
    coalesced_df = large_partition_df.coalesce(4)
    # Trigger action
    coalesced_count = coalesced_df.count()
    coalesced_time = time.time() - start_time
    print(f"Coalesced partition count: {coalesced_df.rdd.getNumPartitions()}")
    print(f"Coalesce duration: {coalesced_time:.2f} seconds")
    
    # Test Repartition (Increasing/shuffling partitions)
    print("\nTesting REPARTITION (20 partitions -> 4 partitions)...")
    start_time = time.time()
    repartitioned_df = large_partition_df.repartition(4)
    repartitioned_count = repartitioned_df.count()
    repartitioned_time = time.time() - start_time
    print(f"Repartitioned partition count: {repartitioned_df.rdd.getNumPartitions()}")
    print(f"Repartition duration: {repartitioned_time:.2f} seconds")
    
    print("\nPhysical Plan for Coalesce:")
    coalesced_df.explain()
    
    print("\nPhysical Plan for Repartition:")
    repartitioned_df.explain()

def run_lab3_caching(spark):
    """
    Lab 3: Caching & Persistence Analysis.
    """
    print("\n========================================================")
    print("LAB 3: Caching and Persistence Performance")
    print("========================================================")
    
    # Generate large DF with expensive computation
    df = spark.range(0, 3000000) \
        .withColumn("heavy_calc", col("id") * rand() + rand() - rand())
    
    # Run 1: Action without cache
    print("Run 1: First action (Count) without caching...")
    start_time = time.time()
    count1 = df.count()
    run1_duration = time.time() - start_time
    print(f"First Action Duration: {run1_duration:.2f} seconds")
    
    print("Run 1: Second action (Count) without caching...")
    start_time = time.time()
    count2 = df.count()
    run2_duration = time.time() - start_time
    print(f"Second Action Duration: {run2_duration:.2f} seconds")
    
    # Run 2: Action with cache
    print("\nCaching the DataFrame...")
    df.cache()
    # Trigger caching by calling an action
    print("Run 2: First action (Count) to populate Cache...")
    start_time = time.time()
    count3 = df.count()
    run3_duration = time.time() - start_time
    print(f"Cache Population Duration: {run3_duration:.2f} seconds")
    
    print("Run 2: Second action (Count) reading from Cache...")
    start_time = time.time()
    count4 = df.count()
    run4_duration = time.time() - start_time
    print(f"Cached Read Duration: {run4_duration:.2f} seconds")
    
    print(f"\nSpeedup Factor on second run: {run2_duration / run4_duration:.1f}x faster!")
    df.unpersist()

def run_lab4_aqe(spark):
    """
    Lab 4: Adaptive Query Execution.
    We show:
    1. Skew Join Handling (when skew Join is enabled vs disabled).
    2. Dynamic Coalescing of partitions.
    """
    print("\n========================================================")
    print("LAB 4: Adaptive Query Execution (AQE) Performance")
    print("========================================================")
    
    # Enable AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    # Enable skew join
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # Enable join optimization
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    
    # Generate skewed tables
    print("Generating skewed transaction dataset...")
    tx_df = spark.range(0, 1000000) \
        .withColumn("join_key", when(col("id") % 100 < 95, "KEY_SKEW").otherwise(col("id").cast("string"))) \
        .withColumn("amount", rand() * 100)
    
    lookup_df = spark.range(0, 1000) \
        .withColumn("join_key", when(col("id") == 0, "KEY_SKEW").otherwise(col("id").cast("string"))) \
        .withColumn("category", lit("CAT_A"))
        
    print("\nExecuting join with AQE active...")
    start_time = time.time()
    
    # Since lookup_df is small (1000 rows), AQE should dynamically convert this
    # SortMergeJoin into a Broadcast Hash Join (BHJ) or optimize the partition skew!
    result_df = tx_df.join(lookup_df, "join_key")
    count = result_df.count()
    
    duration = time.time() - start_time
    print(f"Join completed! Result count: {count}")
    print(f"Time Taken (With AQE): {duration:.2f} seconds")
    
    print("\nPhysical Execution Plan with AQE enabled:")
    result_df.explain()

def run_lab5_shuffle(spark):
    """
    Lab 5: Shuffle Partitions Tuning.
    """
    print("\n========================================================")
    print("LAB 5: Shuffle Partitions Tuning")
    print("========================================================")
    
    df1 = spark.range(0, 50000).withColumn("key", col("id") % 100)
    df2 = spark.range(0, 50000).withColumn("key", col("id") % 100)
    
    # Run 1: Default/High shuffle partitions (e.g. 200)
    print("Running aggregation with 200 shuffle partitions...")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    start_time = time.time()
    res1 = df1.join(df2, "key").groupBy("key").count()
    count1 = res1.count()
    duration1 = time.time() - start_time
    print(f"Completed in {duration1:.2f} seconds (200 partitions)")
    
    # Run 2: Optimized shuffle partitions (e.g. 4)
    print("\nRunning aggregation with 4 shuffle partitions...")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    start_time = time.time()
    res2 = df1.join(df2, "key").groupBy("key").count()
    count2 = res2.count()
    duration2 = time.time() - start_time
    print(f"Completed in {duration2:.2f} seconds (4 partitions)")
    
    print(f"\nSpeedup: {duration1 / duration2:.2f}x faster!")

def main():
    parser = argparse.ArgumentParser(description="Spark Performance Tuning Labs")
    parser.add_argument("--lab", type=int, choices=[1, 2, 3, 4, 5], required=True,
                        help="The Lab number to run (1 to 5)")
    args = parser.parse_args()
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        if args.lab == 1:
            run_lab1_skew(spark)
        elif args.lab == 2:
            run_lab2_partitioning(spark)
        elif args.lab == 3:
            run_lab3_caching(spark)
        elif args.lab == 4:
            run_lab4_aqe(spark)
        elif args.lab == 5:
            run_lab5_shuffle(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
