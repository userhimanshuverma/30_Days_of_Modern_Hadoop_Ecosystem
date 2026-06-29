#!/usr/bin/env python3
import os
import sys
import time
import math

def log(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [YARN-DOCKER-APP] {message}")
    sys.stdout.flush()

def print_environment():
    log("=========================================")
    log("🔬 CONTAINER ENVIRONMENT DIAGNOSTICS")
    log("=========================================")
    for key in sorted(os.environ.keys()):
        if any(term in key for term in ["HADOOP", "YARN", "CONTAINER", "USER", "PATH"]):
            log(f"{key} = {os.environ[key]}")
    log("=========================================")

def inspect_cgroups():
    log("Checking active container limits via CGroups...")
    memory_limit_path = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
    if os.path.exists(memory_limit_path):
        try:
            with open(memory_limit_path, "r") as f:
                limit = int(f.read().strip())
                log(f"CGroups Memory Limit: {limit / (1024*1024):.2f} MB")
        except Exception as e:
            log(f"Failed to read memory limit: {e}")
    else:
        log("Memory controller path not found (perhaps using unified CGroups v2).")

def execute_workload():
    log("Starting data simulation workload...")
    # Run a simple CPU intensive calculation (calculating Pi using Leibniz formula)
    n = 20000000
    log(f"Calculating Pi approximations over {n:,} iterations to simulate compute phase...")
    
    start_time = time.time()
    pi_estimate = 0
    for i in range(n):
        pi_estimate += ((-1)**i) / (2*i + 1)
    pi_estimate *= 4
    
    elapsed = time.time() - start_time
    log(f"Workload complete in {elapsed:.3f} seconds.")
    log(f"Calculated Pi value: {pi_estimate:.10f} (Error: {abs(pi_estimate - math.pi):.10f})")

def main():
    log("Initializing containerized application inside YARN container runtime.")
    print_environment()
    inspect_cgroups()
    execute_workload()
    log("Application shutting down successfully. Exiting code 0.")

if __name__ == "__main__":
    main()
