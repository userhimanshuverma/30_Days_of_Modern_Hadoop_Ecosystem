#!/usr/bin/env bash
# docker/entrypoint.sh
set -eo pipefail

echo "🚀 Initializing Day 28 - Platform Observability Stack Environment..."

# Ensure config permissions
chmod 644 /etc/prometheus/*.yml 2>/dev/null || true
chmod 644 /etc/alertmanager/*.yml 2>/dev/null || true

echo "🟢 Initialization complete. Launching target service..."
exec "$@"
