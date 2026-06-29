FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy processing script
COPY process.py /app/process.py

# Define command
CMD ["python", "/app/process.py"]
