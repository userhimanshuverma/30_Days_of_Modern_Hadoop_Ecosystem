#!/usr/bin/env python3
"""
service_discovery.py
A production-ready service registry and discovery example using Apache ZooKeeper.
Demonstrates ephemeral-sequential nodes for registration and Watcher for dynamic discovery.
"""

import sys
import time
import logging
import uuid
from kazoo.client import KazooClient
from kazoo.client import KazooState

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

SERVICES_PATH = "/services/user-service"
HOSTS = "localhost:2181,localhost:2182,localhost:2183"
INSTANCE_ID = str(uuid.uuid4())[:8]
HOST_IP = "172.20.0.101"
PORT = 8080

def main():
    logger.info(f"Starting service instance {INSTANCE_ID} connecting to ZooKeeper...")
    zk = KazooClient(hosts=HOSTS, timeout=10.0)
    
    try:
        zk.start()
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)
        
    # Ensure root services path exists
    zk.ensure_path(SERVICES_PATH)
    
    # 1. REGISTER: Create an ephemeral, sequential node representing this service instance
    node_path = f"{SERVICES_PATH}/instance_"
    node_data = f"{HOST_IP}:{PORT}".encode('utf-8')
    
    logger.info(f"Registering service node at path {node_path}...")
    registered_path = zk.create(
        node_path, 
        node_data, 
        ephemeral=True, 
        sequence=True
    )
    logger.info(f"Successfully registered: {registered_path}")
    
    # 2. DISCOVER & WATCH: Set a watch on the children of user-service path
    @zk.ChildrenWatch(SERVICES_PATH)
    def watch_instances(children):
        logger.info("=== [DISCOVERY UPDATE] Live service instances modified ===")
        if not children:
            logger.info("No service instances currently available.")
            return
            
        logger.info(f"Active Instances Count: {len(children)}")
        for child in children:
            child_path = f"{SERVICES_PATH}/{child}"
            try:
                data, stat = zk.get(child_path)
                logger.info(f" - {child}: Endpoint -> {data.decode('utf-8')}")
            except Exception as e:
                # Node might have been deleted between child retrieval and data read
                logger.warning(f"Could not read data for {child_path}: {e}")
                
    try:
        # Keep client running to maintain ephemeral node and watch session
        logger.info("Service is online. Press Ctrl+C to terminate.")
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Shutdown initiated by user.")
    finally:
        logger.info("Unregistering service (dropping session)...")
        zk.stop()
        zk.close()
        logger.info("Service offline.")

if __name__ == "__main__":
    main()
