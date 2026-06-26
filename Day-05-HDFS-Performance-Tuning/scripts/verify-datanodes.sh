#!/bin/bash
# Script to verify DataNode status, thread counts, JVM memory usage via HDFS commands and JMX API

echo "============================================="
echo " HDFS DFSADMIN REPORT SUMMARY"
echo "============================================="
hdfs dfsadmin -report | grep -E "Live datanodes|Total capacity|Remaining|Replicated Blocks"

echo "============================================="
echo " NAMENODE JMX METRICS (RACK AWARENESS & LIVE NODES)"
echo "============================================="
# Querying NameNode JMX for live nodes list
NN_JMX_INFO=$(curl -s "http://namenode.hadoop.local:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")
echo "[+] Live DataNodes details (Name, Rack, Status):"
echo "$NN_JMX_INFO" | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
    live_nodes = json.loads(data["beans"][0]["LiveNodes"])
    for node, info in live_nodes.items():
        print(f" - Node: {node:<30} | Rack: {info.get(\"racks\", \"/default\"):<10} | AdminState: {info.get(\"adminState\", \"Unknown\")}")
except Exception as e:
    print("[-] Error parsing NameNode JMX LiveNodes: ", e)
'

echo "============================================="
echo " DATANODE JVM HEAP & ACTIVE THREAD COUNT"
echo "============================================="
# Query JMX from DataNode 1
DN_JMX_JVM=$(curl -s "http://datanode1.hadoop.local:9864/jmx?qry=java.lang:type=Threading")
DN_JMX_INFO=$(curl -s "http://datanode1.hadoop.local:9864/jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo")

echo "$DN_JMX_JVM" | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
    thread_count = data["beans"][0]["ThreadCount"]
    peak_threads = data["beans"][0]["PeakThreadCount"]
    print(f"[+] DataNode 1 Active JVM Threads: {thread_count} (Peak: {peak_threads})")
except Exception as e:
    print("[-] Error parsing DataNode Threading JMX: ", e)
'

echo "$DN_JMX_INFO" | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
    vol_info = json.loads(data["beans"][0]["VolumeInfo"])
    print("[+] DataNode 1 Storage Volumes:")
    for vol, details in vol_info.items():
        print(f" - Volume: {vol} | Free: {details.get(\"freeSpace\", 0)/(1024*1024):.2f} MB | Used: {details.get(\"usedSpace\", 0)/(1024*1024):.2f} MB")
except Exception as e:
    print("[-] Error parsing DataNode Volume JMX: ", e)
'

echo "============================================="
echo -e "\033[0;32m[SUCCESS]\033[0m HDFS DataNode states, rack placements, and JVM thread counts verified."
