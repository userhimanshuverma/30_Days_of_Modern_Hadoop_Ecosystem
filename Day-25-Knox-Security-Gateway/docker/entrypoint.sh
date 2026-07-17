#!/usr/bin/env bash
# Entrypoint script for Apache Knox Container

set -eo pipefail

echo "================================================================="
echo " Starting Apache Knox Security Gateway                          "
echo "================================================================="

# Path to configs
KNOX_CONF="${KNOX_HOME}/conf"

# Copy configurations and topologies from the mounted configs and topologies directories if available
if [ -d "/mnt/configs" ] && [ "$(ls -A /mnt/configs)" ]; then
    echo "Applying custom configurations from /mnt/configs..."
    cp -rf /mnt/configs/* "${KNOX_CONF}/"
fi

if [ -d "/mnt/topologies" ] && [ "$(ls -A /mnt/topologies)" ]; then
    echo "Applying custom topologies from /mnt/topologies..."
    mkdir -p "${KNOX_CONF}/topologies"
    cp -rf /mnt/topologies/* "${KNOX_CONF}/topologies/"
fi

# Ensure master secret file exists or create it
MASTER_SECRET_FILE="${KNOX_HOME}/data/security/master"
if [ ! -f "${MASTER_SECRET_FILE}" ]; then
    echo "Generating new Apache Knox master secret..."
    # Set default master secret if not provided in environment
    KNOX_MASTER_SECRET=${KNOX_MASTER_SECRET:-"knoxmastersecret123!"}
    
    # Run CLI command to initialize master secret
    ${KNOX_HOME}/bin/knox-cli.sh create-master --master "${KNOX_MASTER_SECRET}"
    echo "Master secret successfully generated."
else
    echo "Knox master secret already exists."
fi

# Ensure Knox Gateway SSL self-signed certificate is created (if JKS keystore doesn't exist)
KEYSTORE_FILE="${KNOX_HOME}/data/security/keystores/gateway.jks"
if [ ! -f "${KEYSTORE_FILE}" ]; then
    echo "Generating default SSL keystore and self-signed certificate..."
    ${KNOX_HOME}/bin/knox-cli.sh create-cert --hostname localhost
    echo "SSL Certificate generated successfully."
else
    echo "SSL Certificate keystore already exists."
fi

echo "Starting Apache Knox Gateway in the foreground..."
# Execute the gateway process directly to receive termination signals (SIGTERM/SIGINT) as PID 1
exec java -jar "${KNOX_HOME}/bin/gateway.jar" -conf "${KNOX_CONF}"
