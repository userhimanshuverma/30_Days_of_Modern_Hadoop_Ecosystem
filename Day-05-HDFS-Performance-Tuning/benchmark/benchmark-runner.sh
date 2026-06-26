#!/bin/bash
# HDFS Standard Benchmarking Script using TestDFSIO and nnbench

# Find the Hadoop Jobclient test jar automatically
JAR_PATH=$(find /opt/hadoop/share/hadoop/mapreduce/ -name "hadoop-mapreduce-client-jobclient-*-tests.jar" | head -n 1)

if [ -z "$JAR_PATH" ]; then
    echo "[-] Error: Hadoop mapreduce client jobclient tests jar not found!"
    exit 1
fi

echo "[+] Found HDFS Benchmarking JAR at: $JAR_PATH"

echo "============================================="
echo " RUNNING TESTDFSIO WRITE BENCHMARK (Throughput)"
echo "============================================="
# Write 5 files of 100MB each
hadoop jar "$JAR_PATH" TestDFSIO -write -nrFiles 5 -fileSize 100MB -resFile /tmp/TestDFSIO_write.txt

echo "[+] Write Results:"
cat /tmp/TestDFSIO_write.txt

echo "============================================="
echo " RUNNING TESTDFSIO READ BENCHMARK (Throughput)"
echo "============================================="
# Read 5 files of 100MB each
hadoop jar "$JAR_PATH" TestDFSIO -read -nrFiles 5 -fileSize 100MB -resFile /tmp/TestDFSIO_read.txt

echo "[+] Read Results:"
cat /tmp/TestDFSIO_read.txt

echo "============================================="
echo " RUNNING NNBENCH BENCHMARK (NameNode Operations)"
echo "============================================="
# Runs nnbench to stress-test NameNode RPC operations (creates and writes)
# 4 maps, 2 reduces, 1000 files
hadoop jar "$JAR_PATH" nnbench -operation create_write -maps 4 -reduces 2 -numberOfFiles 1000 -dir /benchmarks/nnbench -resFile /tmp/nnbench_results.txt

echo "[+] NameNode Benchmark Results:"
cat /tmp/nnbench_results.txt

echo "============================================="
echo " CLEANING UP BENCHMARK DATA"
echo "============================================="
hadoop jar "$JAR_PATH" TestDFSIO -clean
hdfs dfs -rm -r -f /benchmarks/nnbench

echo "[+] Benchmark run completed successfully!"
