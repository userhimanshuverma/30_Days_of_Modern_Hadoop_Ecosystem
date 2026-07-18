#!/usr/bin/env bash
# verify-audit.sh
# Queries Apache Solr to check for Ranger Audit logs.

set -eo pipefail

SOLR_URL=${1:-"http://localhost:8983/solr"}
CORE_NAME="ranger_audits"

echo "=========================================================="
echo "🛡️ Checking Ranger Access Audits in Solr..."
echo "=========================================================="

# 1. Check Solr core availability
echo "🔍 Checking status of Solr core '${CORE_NAME}'..."
CORE_STATUS=$(curl -s "${SOLR_URL}/admin/cores?action=STATUS&core=${CORE_NAME}")

if echo "${CORE_STATUS}" | grep -q "instanceDir"; then
    echo "✅ [Solr Core] - '${CORE_NAME}' is loaded and active."
else
    echo "❌ [Solr Core] - '${CORE_NAME}' is not loaded in Solr."
    echo "Check if Solr is running and the core was successfully pre-created."
    exit 1
fi

# 2. Query Audits
echo "📥 Querying recent access logs..."
AUDITS_JSON=$(curl -s "${SOLR_URL}/${CORE_NAME}/select?q=*:*&sort=evtTime%20desc&rows=5&wt=json")

NUM_FOUND=$(echo "${AUDITS_JSON}" | grep -o '"numFound":[0-9]*' | cut -d':' -f2 || echo "0")

echo "📊 Total indexed audit logs in Solr: ${NUM_FOUND}"

if [ "${NUM_FOUND}" -gt 0 ]; then
    echo "✅ Recent access audit entries (JSON details):"
    echo "${AUDITS_JSON}" | grep -E '"(reqUser|resType|accessType|result|evtTime|resource)"' | sed 's/^[ \t]*//' || true
else
    echo "ℹ️ Audit log is empty. Trigger access to HDFS/Hive files to generate audit logs."
fi

echo "=========================================================="
echo "🎯 Audit Log Verification Completed!"
echo "=========================================================="
