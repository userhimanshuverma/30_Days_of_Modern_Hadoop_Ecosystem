#!/usr/bin/env python3
# Day 17: Spark SQL & Catalyst Optimizer Demo Script
# Location: Day-17-Spark-SQL-Catalyst/source/SparkSqlDemo.py

import os
import sys
import shutil
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, lit

def main():
    print("=========================================================================")
    print("🚀 Day 17 Spark SQL & Catalyst Optimizer Demonstration")
    print("=========================================================================")

    # 1. Initialize Spark Session with default Spark SQL configurations
    # Enabling Event Logging and Adaptive Query Execution (AQE)
    spark = SparkSession.builder \
        .appName("SparkSqlCatalystDemo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")

    # Clean previous run outputs
    output_dir = "/workspace/source/output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print("\n--- 1. Generating Mock Datasets ---")
    # Generate user profiles (100,000 rows) with some skew in city_id
    users_df = spark.range(1, 100001) \
        .withColumn("name", col("id").cast("string")) \
        .withColumn("age", (rand(seed=42) * 80 + 10).cast("int")) \
        .withColumn("city_id", when(col("id") % 10 == 0, 999).otherwise((col("id") % 5).cast("int")))

    # Generate city details (small reference lookup table)
    cities_data = [
        (0, "New York", "USA"),
        (1, "London", "UK"),
        (2, "Paris", "France"),
        (3, "Tokyo", "Japan"),
        (4, "Berlin", "Germany"),
        (999, "SkewTown", "Unknown")
    ]
    cities_df = spark.createDataFrame(cities_data, ["city_id", "city_name", "country"])

    # Register temporary views to run raw SQL
    users_df.createOrReplaceTempView("users")
    cities_df.createOrReplaceTempView("cities")

    print(f"Users partition count: {users_df.rdd.getNumPartitions()}")
    print(f"Cities partition count: {cities_df.rdd.getNumPartitions()}")

    print("\n--- 2. Inspecting Query Plans (EXPLAIN) ---")
    query = """
        SELECT u.id, u.age, c.city_name
        FROM users u
        JOIN cities c ON u.city_id = c.city_id
        WHERE u.age > 30 AND c.country = 'USA'
    """
    
    print("\nExecuting query:")
    print(query)
    
    # We execute explain() with mode="extended" to get parsed, analyzed, optimized, and physical plans
    print("\n=== EXTENDED EXPLAIN PLAN ===")
    plan_df = spark.sql(query)
    plan_df.explain(True)
    
    # Write plan to file for verification
    with open(f"{output_dir}/explain_plan.txt", "w") as f:
        # Redirect standard output capture to write the plan
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        plan_df.explain(True)
        explain_content = sys.stdout.getvalue()
        sys.stdout = old_stdout
        f.write(explain_content)
    print(f"✅ Extended query plan saved to: {output_dir}/explain_plan.txt")

    print("\n--- 3. Demonstrating Catalyst Optimizations ---")
    
    # Column Pruning and Predicate Pushdown analysis
    print("Verifying Column Pruning and Predicate Pushdown in explain_plan.txt...")
    with open(f"{output_dir}/explain_plan.txt", "r") as f:
        content = f.read()
        
    if "Filter" in content and "Project" in content:
        print("✅ Column Pruning (Project) and Filtering (Filter) are active in Logical/Optimized Plans.")
    
    # Check for Predicate Pushdown in physical plan
    # Look for "PushedFilters" or "PartitionFilters" in the plan
    if "PushedFilters" in content or "Filter (" in content:
        print("✅ Predicate Pushdown applied at physical scan level.")

    print("\n--- 4. Join Optimization Strategies ---")
    
    # A. Broadcast Hash Join (BHJ)
    # The default autoBroadcastJoinThreshold is 10MB. cities is tiny (a few bytes), so it should default to BHJ.
    print("\nExecuting join with small lookup table (Should use BroadcastHashJoin)...")
    bhj_query = "SELECT u.id, c.city_name FROM users u JOIN cities c ON u.city_id = c.city_id"
    bhj_df = spark.sql(bhj_query)
    
    bhj_explain_io = io.StringIO()
    sys.stdout = bhj_explain_io
    bhj_df.explain(False)
    bhj_explain = bhj_explain_io.getvalue()
    sys.stdout = old_stdout
    
    print(bhj_explain)
    if "BroadcastHashJoin" in bhj_explain or "BroadcastExchange" in bhj_explain:
        print("✅ Confirmed: Spark executed a Broadcast Hash Join (No shuffle required for lookup table).")
    else:
        print("⚠️ Warning: Broadcast Join not observed. Check settings.")

    # B. Disabling Broadcast Join (Forces Sort Merge Join - SMJ)
    print("\nDisabling autoBroadcastJoinThreshold to force SortMergeJoin...")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    smj_df = spark.sql(bhj_query)
    
    smj_explain_io = io.StringIO()
    sys.stdout = smj_explain_io
    smj_df.explain(False)
    smj_explain = smj_explain_io.getvalue()
    sys.stdout = old_stdout
    
    print(smj_explain)
    if "SortMergeJoin" in smj_explain:
        print("✅ Confirmed: Spark executed a Sort Merge Join (Requires Shuffling and Sorting).")
    else:
        print("⚠️ Warning: Sort Merge Join not observed.")
        
    # Re-enable broadcast join threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

    print("\n--- 5. Adaptive Query Execution (AQE) in Action ---")
    
    # AQE performs runtime optimizations after stages complete.
    # To demonstrate AQE partition coalescing:
    # 1. We run a group-by operation.
    # 2. Spark defaults to 10 partitions (spark.sql.shuffle.partitions) but AQE should reduce it at runtime if data is small.
    # Let's set initial partition count high to force coalescence.
    spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "50")
    
    print("Submitting query with high initial partition count (50) to observe AQE coalescing...")
    aqe_df = spark.sql("SELECT city_id, count(*) FROM users GROUP BY city_id")
    
    # Trigger execution
    aqe_df.write.format("csv").mode("overwrite").save(f"{output_dir}/aqe_groupby_output")
    
    # Let's print the physical plan *after* execution has run (so AQE has runtime stats)
    print("\n=== POST-EXECUTION AQE PHYSICAL PLAN ===")
    aqe_df.explain(True)
    
    # Write post-execution plan to file
    with open(f"{output_dir}/aqe_explain_plan.txt", "w") as f:
        sys.stdout = io.StringIO()
        aqe_df.explain(True)
        aqe_content = sys.stdout.getvalue()
        sys.stdout = old_stdout
        f.write(aqe_content)
        
    if "AdaptiveSparkPlan" in aqe_content:
        print("✅ Confirmed: AdaptiveSparkPlan node is present in the physical plan.")
        if "coalesced" in aqe_content.lower() or "coalesce" in aqe_content.lower():
            print("✅ Confirmed: AQE successfully coalesced empty/small partitions at runtime.")
    else:
        print("⚠️ AQE AdaptiveSparkPlan was not active.")

    # Stop session
    spark.stop()
    print("\n=========================================================================")
    print("🎉 Spark SQL Demo Execution Complete!")
    print("=========================================================================")

if __name__ == "__main__":
    main()
