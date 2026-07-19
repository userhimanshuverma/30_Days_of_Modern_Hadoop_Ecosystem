#!/usr/bin/env bash
# scripts/verify-dag.sh
# Performs syntax and import validations on the DAG directory files

set -eo pipefail

DAG_DIR="./dags"
echo "========================================================="
echo "📝 STATIC VALIDATION: PARSING DAG DIRECTORIES"
echo "========================================================="

# Check if directory exists
if [ ! -d "$DAG_DIR" ]; then
    echo "🔴 ERROR: DAG directory '$DAG_DIR' not found."
    exit 1
fi

# 1. Compile each python file locally to detect early syntax issues
echo "🔍 Checking Python compile status..."
for f in "$DAG_DIR"/*.py; do
    if [ -f "$f" ]; then
        echo -n "Checking: $(basename "$f") ... "
        if python -m py_compile "$f" > /dev/null 2>&1; then
            echo "🟢 SYNTAX VALID"
        else
            echo "🔴 SYNTAX ERROR DETECTED!"
            python -m py_compile "$f" || true
            exit 1
        fi
    fi
done

# 2. Check if Docker container is running and execute Airflow import checks
if docker compose ps | grep -q "scheduler"; then
    echo "🐳 Scheduler container is running. Requesting internal import status..."
    IMPORT_ERRORS=$(docker compose exec -T scheduler airflow dags list-import-errors 2>&1)
    
    # Airflow outputs 'No import errors found' if everything is correct
    if echo "$IMPORT_ERRORS" | grep -qi "No import errors found"; then
        echo "🟢 Import Check: NO ERRORS FOUND"
    else
        echo "🔴 IMPORT ERRORS ENCOUNTERED:"
        echo "$IMPORT_ERRORS"
        exit 1
    fi
else
    echo "🟡 Container stack offline. Skipping live scheduler import checks."
fi

echo "========================================================="
echo "DAG static verification successfully finished."
