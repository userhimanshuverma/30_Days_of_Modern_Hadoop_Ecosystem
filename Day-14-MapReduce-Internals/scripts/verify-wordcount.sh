#!/usr/bin/env bash
# verify-wordcount.sh
# Triggers Maven compilation and packaging of the WordCount Java project inside the container environment.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Compiling WordCount Java Project ===${NC}"

# Check if pom.xml exists
if [ ! -f "source/pom.xml" ]; then
  echo -e "${RED}[ERROR] Maven pom.xml not found at: source/pom.xml. Execute this script from the Day-14 root directory.${NC}"
  exit 1
fi

# Run maven package inside the namenode container since it has Maven installed and /workspace mounted
echo "Running 'mvn clean package' inside namenode container..."
BUILD_STATUS=$(docker exec -w /workspace/source namenode-day14 mvn clean package 2>&1 || echo "FAILED")

if echo "$BUILD_STATUS" | grep -q "FAILED"; then
  echo -e "${RED}[ERROR] Maven build failed. Build logs:${NC}"
  echo "$BUILD_STATUS"
  exit 1
fi

# Verify the artifact is generated
JAR_PATH="source/target/wordcount-mapreduce-1.0-SNAPSHOT.jar"
if [ -f "$JAR_PATH" ]; then
  echo -e "${GREEN}[SUCCESS] WordCount MapReduce JAR built successfully at: ${JAR_PATH}${NC}"
  exit 0
else
  echo -e "${RED}[ERROR] Compilation completed but JAR was not found at expected location: ${JAR_PATH}.${NC}"
  exit 1
fi
