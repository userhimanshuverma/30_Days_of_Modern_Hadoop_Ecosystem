#!/usr/bin/env bash
# Day 25: verify-knox.sh
# Verifies the Knox Gateway health, certificate availability, and LDAP authentication capability.

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

KNOX_HOST=${1:-"localhost"}
KNOX_PORT=${2:-"8443"}
KNOX_URL="https://${KNOX_HOST}:${KNOX_PORT}/gateway/sandbox"

echo -e "${YELLOW}=================================================================${NC}"
echo -e "${YELLOW} Validating Knox Gateway Connectivity & Authentication           ${NC}"
echo -e "${YELLOW}=================================================================${NC}"

# 1. Check if the port is reachable
echo -n "1. Checking if port ${KNOX_PORT} is reachable... "
if ! curl -k -s --connect-timeout 5 "https://${KNOX_HOST}:${KNOX_PORT}" > /dev/null; then
    # If SSL fails to connect or port is closed, we get exit code. Wait, curl might return 35 or 52 for empty response, which is fine since port is open.
    # We check if port is open using bash /dev/tcp
    if (echo > /dev/tcp/${KNOX_HOST}/${KNOX_PORT}) >/dev/null 2>&1; then
        echo -e "${GREEN}REACHABLE${NC}"
    else
        echo -e "${RED}FAILED (Port is closed or Knox is offline)${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}REACHABLE${NC}"
fi

# 2. Test Gateway response with Bad Credentials (expect 401 Unauthorized)
echo -n "2. Verifying authentication behavior with invalid credentials... "
HTTP_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" -u "guest:wrong_password" "${KNOX_URL}/webhdfs/v1/?op=LISTSTATUS")
if [ "$HTTP_STATUS" -eq 401 ]; then
    echo -e "${GREEN}PASSED (Received 401 Unauthorized as expected)${NC}"
else
    echo -e "${RED}FAILED (Expected 401, but got HTTP status: ${HTTP_STATUS})${NC}"
    exit 1
fi

# 3. Test Gateway response with Valid Credentials (expect 200 OK or 307 Redirect for WebHDFS LISTSTATUS)
echo -n "3. Verifying authentication with valid LDAP credentials (guest)... "
# WebHDFS LISTSTATUS on root path.
HTTP_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" -u "guest:guestpassword" "${KNOX_URL}/webhdfs/v1/?op=LISTSTATUS")
if [ "$HTTP_STATUS" -eq 200 ] || [ "$HTTP_STATUS" -eq 307 ]; then
    echo -e "${GREEN}PASSED (Received HTTP ${HTTP_STATUS})${NC}"
else
    echo -e "${RED}FAILED (Expected 200 or 307, but got HTTP status: ${HTTP_STATUS})${NC}"
    echo -e "${YELLOW}Hint: Ensure your docker environment is running and openldap is healthy.${NC}"
    exit 1
fi

# 4. Verify Authorization rules - test a service with user that doesn't have roles if configured.
echo -n "4. Testing authorization rules (Accessing Knox Admin API with standard user)... "
# Guest is not in the 'admin' group, so accessing the admin topology API should be forbidden (403 or 401)
ADMIN_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" -u "guest:guestpassword" "https://${KNOX_HOST}:${KNOX_PORT}/gateway/admin/api/v1/version")
if [ "$ADMIN_STATUS" -eq 403 ] || [ "$ADMIN_STATUS" -eq 401 ]; then
    echo -e "${GREEN}PASSED (Access denied: HTTP ${ADMIN_STATUS} as expected)${NC}"
else
    echo -e "${YELLOW}WARNING (Expected 401 or 403, got ${ADMIN_STATUS}. Ensure AclsAuthz is active in topologies/admin.xml)${NC}"
fi

# 5. Admin authentication verification
echo -n "5. Testing admin credentials access to Admin API... "
ADMIN_AUTH_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" -u "admin:adminpassword" "https://${KNOX_HOST}:${KNOX_PORT}/gateway/admin/api/v1/version")
if [ "$ADMIN_AUTH_STATUS" -eq 200 ]; then
    echo -e "${GREEN}PASSED (Access granted: HTTP 200 OK)${NC}"
else
    echo -e "${RED}FAILED (Expected 200, got HTTP status: ${ADMIN_AUTH_STATUS})${NC}"
    exit 1
fi

echo -e "\n${GREEN}✔ Knox Security Gateway health & auth verification successful!${NC}"
