#!/usr/bin/env bash
# submit-multi-tenant-demo.sh
# Simulates a multi-tenant cluster workload by submitting concurrent MapReduce jobs to different queues.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

RM_CONTAINER="resourcemanager-day07"

echo -e "${YELLOW}=== YARN Multi-Tenant Workload Simulator ===${NC}"

# 1. Check container
if ! docker ps --format '{{.Names}}' | grep -q "^${RM_CONTAINER}$"; then
  echo -e "${RED}[ERROR] Container '${RM_CONTAINER}' is not running. Please start the cluster first.${NC}"
  exit 1
fi

# 2. Locate jar
echo "Locating MapReduce examples JAR file..."
JAR_PATH=$(docker exec "${RM_CONTAINER}" find /opt/hadoop /usr/local /opt/hadoop-3.2.1 -name "hadoop-mapreduce-examples-*.jar" 2>/dev/null | head -n 1 || echo "")

if [ -z "$JAR_PATH" ]; then
  JAR_PATH="/opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar"
fi
echo -e "Using examples JAR: ${GREEN}${JAR_PATH}${NC}"

# 3. Create dummy directory for tests if needed
echo -e "\nInitializing test inputs in HDFS..."
docker exec "${RM_CONTAINER}" hdfs dfs -mkdir -p /user/root/input || true
docker exec "${RM_CONTAINER}" hdfs dfs -rm -r /user/root/input/data-demo* 2>/dev/null || true
docker exec "${RM_CONTAINER}" hdfs dfs -put -f /etc/hosts /user/root/input/data-demo || true

# 4. Submit background jobs to different queues
echo -e "\n${YELLOW}Submitting Concurrent Jobs to Multiple Queues...${NC}"

echo -e "🚀 [Job 1] Submitting Finance MR Pi Job to \033[1;34mroot.prod.finance\033[0m (Capacity: 24%)"
docker exec -d "${RM_CONTAINER}" yarn jar "${JAR_PATH}" pi -Dmapreduce.job.queuename=root.prod.finance 8 100

echo -e "🚀 [Job 2] Submitting Marketing MR Pi Job to \033[1;34mroot.prod.marketing\033[0m (Capacity: 36%)"
docker exec -d "${RM_CONTAINER}" yarn jar "${JAR_PATH}" pi -Dmapreduce.job.queuename=root.prod.marketing 8 100

echo -e "🚀 [Job 3] Submitting DataScience MR Pi Job to \033[1;34mroot.dev.datascience\033[0m (Capacity: 20%)"
docker exec -d "${RM_CONTAINER}" yarn jar "${JAR_PATH}" pi -Dmapreduce.job.queuename=root.dev.datascience 8 100

echo -e "🚀 [Job 4] Submitting Sandbox default MR Pi Job to \033[1;34mroot.default\033[0m (Capacity: 20%)"
docker exec -d "${RM_CONTAINER}" yarn jar "${JAR_PATH}" pi -Dmapreduce.job.queuename=root.default 8 100

echo -e "\n${GREEN}[SUCCESS] All multi-tenant test jobs have been submitted to YARN Scheduler!${NC}"
echo -e "Monitoring current applications..."

# Loop slightly to show state transitions
for i in {1..5}; do
  echo -e "\n--- Application Status Scan ($i/5) ---"
  docker exec "${RM_CONTAINER}" yarn application -list
  sleep 3
done

echo -e "\nTo monitor queue allocations live, check the ResourceManager Web UI at: ${YELLOW}http://localhost:8088/ui2/#/yarn-queues${NC}"
echo -e "Or run diagnostics script: ${GREEN}./verify-queues.sh${NC}"
exit 0
