#!/usr/bin/env bash
# run-wordcount-demo.sh
# End-to-end execution of the MapReduce WordCount pipeline: builds code, uploads data, runs job, prints outputs.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================================================${NC}"
echo -e "${YELLOW}🚀 DAY 14: MAPREDUCE WORDCOUNT PIPELINE DEMO${NC}"
echo -e "${YELLOW}========================================================================${NC}"

# 1. Verify cluster health
echo -e "\n${YELLOW}[Step 1] Verifying Hadoop cluster health...${NC}"
./scripts/verify-hadoop.sh

# 2. Compile Java source code
echo -e "\n${YELLOW}[Step 2] Compiling and packaging Java WordCount job...${NC}"
./scripts/verify-wordcount.sh

# 3. Setup HDFS input directory and upload file
echo -e "\n${YELLOW}[Step 3] Preparing HDFS input directories...${NC}"
# Delete output folders if they exist
docker exec namenode-day14 hdfs dfs -rm -r -f /input /output 2>/dev/null || true

# Recreate folders and upload the sample text file
docker exec namenode-day14 hdfs dfs -mkdir -p /input
docker exec namenode-day14 hdfs dfs -put /workspace/examples/wordcount-input.txt /input/

echo -e "${GREEN}[OK] Sample dataset uploaded to HDFS /input/wordcount-input.txt${NC}"

# 4. Run the MapReduce job on YARN
echo -e "\n${YELLOW}[Step 4] Submitting MapReduce WordCount Job to YARN...${NC}"
# Set environment variables inside the jar driver execution
docker exec namenode-day14 yarn jar /workspace/source/target/wordcount-mapreduce-1.0-SNAPSHOT.jar com.hadoop.mapreduce.WordCount /input /output

# 5. Verify and display output
echo -e "\n${YELLOW}[Step 5] Validating output data...${NC}"
./scripts/verify-output.sh

echo -e "\n${GREEN}========================================================================${NC}"
echo -e "${GREEN}🎉 MAPREDUCE WORDCOUNT JOB COMPLETED SUCCESSFULLY!${NC}"
echo -e "${GREEN}========================================================================${NC}"
exit 0
