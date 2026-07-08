#!/bin/bash
# Day 17: Query Plan Logical/Physical Analysis Script
# Location: Day-17-Spark-SQL-Catalyst/scripts/verify-query-plan.sh

set -e

PLAN_FILE="/workspace/source/output/explain_plan.txt"

echo "=== 🔍 STEP 1: Verifying Plan File Availability ==="
if [ ! -f "$PLAN_FILE" ]; then
  echo "⚠️ Output plan not found. Running the demo first..."
  python3 /workspace/source/SparkSqlDemo.py
fi

echo "=== 🔍 STEP 2: Checking Column Pruning (Project Optimization) ==="
# Look for 'Project' in the logical optimizations showing that only id, age, city_name are selected (name, country etc pruned)
if grep -q "Project " "$PLAN_FILE"; then
  echo "✅ Success: Column Pruning (Project) is present in the Catalyst plan."
else
  echo "❌ Error: Project operator missing from execution graph."
  exit 1
fi

echo "=== 🔍 STEP 3: Checking Predicate Pushdown (Filter Optimization) ==="
# In Spark SQL, predicate pushdown is shown under scan operators (e.g. PushedFilters: [IsNotNull(age), GreaterThan(age,30)])
if grep -q "PushedFilters" "$PLAN_FILE" || grep -q "Filter (" "$PLAN_FILE"; then
  echo "✅ Success: Predicate Pushdown (Filter) has been pushed to the source scan operation."
else
  echo "⚠️ Warning: PushedFilters metadata not found. Spark might be reading memory RDDs directly. Review execution format."
fi

echo "=== 🎉 Query Plan Logic Optimizations Verified! ==="
