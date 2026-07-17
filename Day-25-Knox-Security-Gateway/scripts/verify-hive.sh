#!/usr/bin/env bash
# Day 25: verify-hive.sh
# Validates Hive queries executed through the Apache Knox Gateway using Beeline JDBC over HTTP.

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

KNOX_HOST=${1:-"localhost"}
KNOX_PORT=${2:-"8443"}
USER=${3:-"guest"}
PASSWORD=${4:-"guestpassword"}

echo -e "${YELLOW}=================================================================${NC}"
echo -e "${YELLOW} Validating Hive JDBC Connection via Knox HTTP Gateway           ${NC}"
echo -e "${YELLOW}=================================================================${NC}"

# Define temporary certificate and truststore locations
CERT_PATH="/tmp/knox-gateway.crt"
TRUSTSTORE_PATH="/tmp/knox-client-truststore.jks"
TRUSTSTORE_PASS="changeit"

# 1. Download SSL certificate from Knox Gateway
echo "1. Fetching SSL certificate from Knox Gateway at ${KNOX_HOST}:${KNOX_PORT}..."
if ! openssl s_client -connect "${KNOX_HOST}:${KNOX_PORT}" -showcerts </dev/null 2>/dev/null | openssl x509 -outform PEM > "$CERT_PATH"; then
    echo -e "${RED}FAILED: Could not download SSL certificate. Is Knox Gateway running?${NC}"
    exit 1
fi
echo -e "   -> Saved certificate to ${GREEN}${CERT_PATH}${NC}"

# 2. Build local truststore for Beeline
echo "2. Importing certificate into a temporary client truststore..."
rm -f "$TRUSTSTORE_PATH"
if ! keytool -importcert -noprompt \
    -alias knox-gateway \
    -file "$CERT_PATH" \
    -keystore "$TRUSTSTORE_PATH" \
    -storepass "$TRUSTSTORE_PASS" 2>/dev/null; then
    echo -e "${RED}FAILED: Failed to create truststore JKS using keytool.${NC}"
    exit 1
fi
echo -e "   -> Created truststore JKS at ${GREEN}${TRUSTSTORE_PATH}${NC}"

# 3. Construct JDBC connection string
# Format: jdbc:hive2://<host>:<port>/;ssl=true;sslTrustStore=<path>;trustStorePassword=<pass>;transportMode=http;httpPath=gateway/sandbox/hive
JDBC_URL="jdbc:hive2://${KNOX_HOST}:${KNOX_PORT}/default;ssl=true;sslTrustStore=${TRUSTSTORE_PATH};trustStorePassword=${TRUSTSTORE_PASS};transportMode=http;httpPath=gateway/sandbox/hive"

echo -e "\n3. Executing test query via Beeline..."
echo -e "Connecting to: ${YELLOW}${JDBC_URL}${NC} as ${YELLOW}${USER}${NC}"

# Execute Beeline CLI query
if beeline -u "$JDBC_URL" -n "$USER" -p "$PASSWORD" -e "SHOW DATABASES; SHOW TABLES;" > /tmp/beeline-query.log 2>&1; then
    echo -e "${GREEN}PASSED: Beeline successfully executed query through Knox!${NC}"
    echo -e "\nQuery Results Output:"
    cat /tmp/beeline-query.log | grep -A 10 -i "database_name" || cat /tmp/beeline-query.log
else
    echo -e "${RED}FAILED: Beeline query failed.${NC}"
    echo -e "Check full logs at /tmp/beeline-query.log"
    echo -e "\nError snippet from Beeline:"
    tail -n 15 /tmp/beeline-query.log
    exit 1
fi

# Cleanup temporary certificate and keystore
rm -f "$CERT_PATH" "$TRUSTSTORE_PATH"
echo -e "\n${GREEN}✔ Hive over Knox validation completed successfully!${NC}"
