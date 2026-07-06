#!/bin/bash
# verify-tez.sh - Verifies Apache Tez libraries upload and classpaths.

set -e

echo "=== 🔍 STEP 1: Verifying HDFS filesystem directory structures for Tez ==="
if hadoop fs -test -d /apps/tez; then
    echo "✅ Success: /apps/tez directory exists in HDFS."
else
    echo "❌ Error: /apps/tez directory not found. Running initialization..."
    hadoop fs -mkdir -p /apps/tez
fi

echo "=== 🔍 STEP 2: Verifying Tez distribution tarball in HDFS ==="
if hadoop fs -test -f /apps/tez/tez-0.10.2.tar.gz; then
    echo "✅ Success: tez-0.10.2.tar.gz exists in HDFS."
    hadoop fs -ls -h /apps/tez/tez-0.10.2.tar.gz
else
    echo "❌ Error: tez-0.10.2.tar.gz is missing from HDFS."
    if [ -f "/opt/tez/tez-0.10.2.tar.gz" ]; then
        echo "Uploading local Tez tarball to HDFS..."
        hadoop fs -put /opt/tez/tez-0.10.2.tar.gz /apps/tez/
    else
        echo "Failed to locate local Tez tar package at /opt/tez/tez-0.10.2.tar.gz!"
        exit 1
    fi
fi

echo "=== 🔍 STEP 3: Validating Tez Classpaths and Configurations ==="
if [ -d "/opt/tez/conf" ]; then
    echo "✅ Success: Local Tez config directory found."
else
    echo "❌ Error: Tez configs not found at /opt/tez/conf."
fi

echo "=== 🔍 STEP 4: Compiling local Tez Java Application using Maven ==="
cd /workspace/source
if [ -f "pom.xml" ]; then
    echo "Building Tez application jar..."
    mvn clean package -DskipTests
    echo "✅ Success: Java application compiled."
    ls -l target/*.jar
else
    echo "❌ Error: pom.xml not found at /workspace/source."
    exit 1
fi

echo "=== 🎉 Verification Completed Successfully! ==="
