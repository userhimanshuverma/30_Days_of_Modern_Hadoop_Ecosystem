#!/usr/bin/env python3
"""
config_watcher.py
A production-grade configuration watcher implementation using Apache ZooKeeper.
Shows how applications subscribe to config updates and dynamically reload parameters without restart.
"""

import sys
import time
import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

CONFIG_PATH = "/config/app_settings"
HOSTS = "localhost:2181,localhost:2182,localhost:2183"

def main():
    logger.info("Initializing configuration watcher...")
    zk = KazooClient(hosts=HOSTS, timeout=10.0)
    
    try:
        zk.start()
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)
        
    # Ensure config path exists with default data
    if not zk.exists(CONFIG_PATH):
        zk.ensure_path("/config")
        zk.create(CONFIG_PATH, b"db_pool_size=10\ncache_enabled=true")
        logger.info(f"Initialized configuration path {CONFIG_PATH} with default values.")

    # Define the data watch callback
    # The callback receives data and the stat of the znode
    @zk.DataWatch(CONFIG_PATH)
    def watch_config(data, stat):
        logger.info("=== [CONFIG UPDATE] Configuration node modified ===")
        if data is None:
            logger.warning(f"Config node {CONFIG_PATH} has been deleted!")
            return
            
        config_str = data.decode('utf-8')
        logger.info("Active Configuration Data:")
        for line in config_str.splitlines():
            logger.info(f"  -> {line}")
        logger.info(f"ZNode Metadata: Version={stat.version}, Last Modified={time.ctime(stat.mtime / 1000.0)}")

    try:
        logger.info("Configuration monitor active. Modify the node using zkCli.sh to test. Press Ctrl+C to stop.")
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Stopping configuration watcher...")
    finally:
        zk.stop()
        zk.close()
        logger.info("Configuration watcher closed.")

if __name__ == "__main__":
    main()
