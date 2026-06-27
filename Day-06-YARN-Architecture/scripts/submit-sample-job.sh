#!/usr/bin/env bash
# submit-sample-job.sh
# Submits a sample MapReduce Pi calculation job to the YARN cluster.

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

RM_CONTAINER="resourcemanager-day06"

echo -e "${YELLOW}=== YARN Job Submission Handler ===${NC}"

# Check if RM container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${RM_CONTAINER}$"; then
  echo -e "${RED}[ERROR] Container '${RM_CONTAINER}' is not running. Please start the cluster first.${NC}"
  exit 1
fi

# Locate the examples jar inside the container
echo "Locating MapReduce examples JAR file..."
JAR_PATH=$(docker exec "${RM_CONTAINER}" find /opt/hadoop /usr/local /opt/hadoop-3.2.1 -name "hadoop-mapreduce-examples-*.jar" 2>/dev/null | head -n 1 || echo "")

if [ -z "$JAR_PATH" ]; then
  # Fallback to standard path if find fails
  JAR_PATH="/opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar"
fi

echo -e "Using examples JAR: ${GREEN}${JAR_PATH}${NC}"

# Submit the Pi job
echo -e "\nSubmitting MapReduce Pi job (5 maps, 10 samples per map) to YARN..."
echo -e "${YELLOW}Command: yarn jar ${JAR_PATH} pi 5 10${NC}\n"

# Run the command and capture output
docker exec -it "${RM_CONTAINER}" yarn jar "${JAR_PATH}" pi 5 10

echo -e "\n${GREEN}[SUCCESS] MapReduce Job submitted and executed successfully!${NC}"
echo -e "You can view the job run details on the HistoryServer UI at: ${YELLOW}http://localhost:19888${NC}"
echo -e "You can monitor the active/completed ResourceManager stats at: ${YELLOW}http://localhost:8088${NC}"
exit 0
