#!/bin/bash
# verify-tls.sh - Verifies HTTPS endpoints, SSL handshake validity, and CA trust.
set -eo pipefail

echo "============================================="
echo "🔒 Verifying SSL/TLS Connections..."
echo "============================================="

CA_PEM="/var/ssl/ca-cert.pem"

if [ ! -f "$CA_PEM" ]; then
    echo "❌ [ERROR] CA certificate not found at $CA_PEM"
    exit 1
fi

# 1. Verify NameNode HTTPS Web UI
echo -n "Checking NameNode HTTPS (9871) Web interface... "
NN_HTTP_STATUS=$(curl --cacert "$CA_PEM" -s -o /dev/null -w "%{http_code}" https://namenode.hadoop.local:9871/)
if [ "$NN_HTTP_STATUS" -eq 200 ] || [ "$NN_HTTP_STATUS" -eq 401 ]; then
    # 401 is acceptable if SPNEGO auth is enforced
    echo "✔ [OK] SSL Handshake Successful (Status: $NN_HTTP_STATUS)"
else
    echo "❌ [ERROR] Failed to connect to NameNode HTTPS. Status: $NN_HTTP_STATUS"
    exit 1
fi

# 2. Verify KMS HTTPS API
echo -n "Checking KMS HTTPS (9600) service endpoint... "
KMS_HTTP_STATUS=$(curl --cacert "$CA_PEM" -s -o /dev/null -w "%{http_code}" https://kms-server.hadoop.local:9600/kms/index.html)
if [ "$KMS_HTTP_STATUS" -eq 200 ] || [ "$KMS_HTTP_STATUS" -eq 401 ]; then
    echo "✔ [OK] SSL Handshake Successful (Status: $KMS_HTTP_STATUS)"
else
    echo "❌ [ERROR] Failed to connect to KMS HTTPS. Status: $KMS_HTTP_STATUS"
    exit 1
fi

# 3. Test KMS SPNEGO Negotiate Access
echo -n "Testing Kerberos SPNEGO access to KMS Key list... "
# The client container should be run after a kinit
KMS_KEYS_RESP=$(curl --cacert "$CA_PEM" --negotiate -u : -s https://kms-server.hadoop.local:9600/kms/v1/keys)
if echo "$KMS_KEYS_RESP" | grep -q "Authentication required"; then
    echo "❌ [ERROR] SPNEGO Negotiation failed. Response: $KMS_KEYS_RESP"
    exit 1
else
    echo "✔ [OK] Authentication successful!"
fi

echo "✔ [SUCCESS] TLS and HTTPS validation checks passed!"
