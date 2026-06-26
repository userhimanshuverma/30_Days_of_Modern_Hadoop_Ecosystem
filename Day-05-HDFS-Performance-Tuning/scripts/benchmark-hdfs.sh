#!/bin/bash
# Master Benchmark Orchestrator Script for HDFS Performance Tuning

echo "========================================================="
echo "   HDFS PERFORMANCE BENCHMARK ORCHESTRATOR"
echo "========================================================="
echo "[+] Target environment: HDFS Cluster (3x DataNodes)"
echo "[+] Initiating benchmark suite..."
echo ""

# 1. Run standard HDFS throughput benchmarks
echo "[Step 1/3] Running Standard HDFS Throughput Benchmarks (TestDFSIO & nnbench)..."
bash /tmp/benchmark/benchmark-runner.sh
echo "[+] Step 1 Completed."
echo ""

# 2. Run block size comparison benchmark
echo "[Step 2/3] Running HDFS Block Size Performance Comparison..."
bash /tmp/benchmark/compare-blocksizes.sh
echo "[+] Step 2 Completed."
echo ""

# 3. Run small files problem simulation
echo "[Step 3/3] Running Small Files Overhead Simulation..."
python3 /tmp/benchmark/generate-small-files.py
echo "[+] Step 3 Completed."
echo ""

echo "========================================================="
echo "   BENCHMARK RUN COMPLETE"
echo "========================================================="
echo "Observations & Tuning Takeaways:"
echo " 1. Larger HDFS block sizes (128MB/256MB) significantly reduce client/NameNode roundtrips and increase streaming throughput."
echo " 2. Navigating millions of small files (< 1MB) introduces drastic metadata overhead, increasing NN RPC queue latencies and bottlenecking client upload throughput."
echo " 3. Verify real-time metrics and charts in Grafana Dashboard (http://localhost:3000) using the dashboard provisioned."
echo "========================================================="
