#!/usr/bin/env bash
# ==============================================================================
# run-docker-demo.sh
# Automates the building of the sample app Docker image and submission to YARN.
# ==============================================================================

set -eo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

IMAGE_NAME="yarn-sample-app:latest"

echo "=========================================================="
echo "⚡ SUBMITTER: Launching Dockerized Workload on YARN"
echo "=========================================================="

# 1. Build the sample Docker image
echo "Building Docker image '$IMAGE_NAME'..."
if [ -d "../examples/docker-app" ]; then
    docker build -t "$IMAGE_NAME" ../examples/docker-app
elif [ -d "examples/docker-app" ]; then
    docker build -t "$IMAGE_NAME" examples/docker-app
else
    echo -e "${RED}[ERROR] Sample app directory 'examples/docker-app' not found.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Image '$IMAGE_NAME' built successfully.${NC}"

# 2. Check if YARN ResourceManager is responsive
echo "Checking ResourceManager status..."
if ! curl -sf http://localhost:8088/ws/v1/cluster/info &> /dev/null; then
    echo -e "${RED}[WARNING] ResourceManager not reachable at localhost:8088.${NC}"
    echo "Please ensure your Docker Compose cluster is running with: docker-compose up -d"
    exit 1
else
    echo -e "${GREEN}[OK] ResourceManager is responsive.${NC}"
fi

# 3. Locate YARN DistributedShell jar file
# In standard Hadoop deployments, this jar is in share/hadoop/yarn/applications/
echo "Locating DistributedShell JAR file..."
DS_JAR=""
JAR_SEARCH_PATHS=(
    "/opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.2.1.jar"
    "/opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.3.1.jar"
    "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-"*.jar
)

for path in "${JAR_SEARCH_PATHS[@]}"; do
    if ls $path &> /dev/null; then
        DS_JAR=$(ls $path | head -n 1)
        break
    fi
done

if [ -z "$DS_JAR" ]; then
    echo -e "${RED}[WARNING] DistributedShell JAR not found on host. The application should be run from inside the ResourceManager container.${NC}"
    echo "Example submission command inside container:"
    echo "yarn jar /opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.2.1.jar \\"
    echo "  -appname docker-yarn-demo \\"
    echo "  -shell_command \"python3 /app/process.py\" \\"
    echo "  -num_containers 1 \\"
    echo "  -shell_env YARN_CONTAINER_RUNTIME_TYPE=docker \\"
    echo "  -shell_env YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=$IMAGE_NAME"
else
    echo -e "${GREEN}[OK] Found DistributedShell JAR at $DS_JAR${NC}"
    echo "Submitting application to YARN cluster..."
    
    yarn jar "$DS_JAR" \
      -appname "docker-yarn-demo" \
      -shell_command "python3 /app/process.py" \
      -num_containers 1 \
      -container_memory 1024 \
      -container_vcores 1 \
      -shell_env YARN_CONTAINER_RUNTIME_TYPE=docker \
      -shell_env YARN_CONTAINER_RUNTIME_DOCKER_IMAGE="$IMAGE_NAME" \
      -shell_env YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE=true
fi

echo "=========================================================="
echo "Workload submission process finished!"
echo "=========================================================="
