#!/bin/bash
# Day 23: Trino Validation Script - Verify Active Nodes in Cluster
# Location: Day-23-Trino-MPP-Engine/scripts/verify-trino.sh

echo "========================================================="
echo "Verifying Trino Cluster Active Nodes"
echo "========================================================="

# Run query against system catalog
docker exec -t trino-coordinator-day23 trino --execute "SELECT node_id, http_uri, coordinator, state FROM system.runtime.nodes"

if [ $? -eq 0 ]; then
  echo "✔ Trino cluster communication validated successfully."
else
  echo "✘ Failed to query Trino nodes. Check container logs using 'docker logs trino-coordinator-day23'."
  exit 1
fi
