#!/usr/bin/env bash
# docker/entrypoint.sh
# Custom entrypoint wrapper to ensure dependencies are fully responsive

set -eo pipefail

# Hostname and ports to test
DB_HOST="postgres"
DB_PORT="5432"
REDIS_HOST="redis"
REDIS_PORT="6379"

echo "Checking network connectivity to PostgreSQL database..."
while ! nc -z "${DB_HOST}" "${DB_PORT}"; do
  echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
  sleep 2
done
echo "PostgreSQL is online and accepting requests!"

# Check if redis is configured (for Celery executor)
if [ -n "${AIRFLOW__CELERY__BROKER_URL}" ]; then
  echo "Checking network connectivity to Redis broker..."
  while ! nc -z "${REDIS_HOST}" "${REDIS_PORT}"; do
    echo "Waiting for Redis at ${REDIS_HOST}:${REDIS_PORT}..."
    sleep 2
  done
  echo "Redis broker is online and accepting requests!"
fi

# Run the passed arguments (e.g., airflow webserver, airflow scheduler, etc.)
exec "$@"
