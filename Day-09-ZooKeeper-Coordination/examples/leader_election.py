#!/usr/bin/env python3
"""
leader_election.py
A production-grade leader election implementation using Apache ZooKeeper and Kazoo.
Demonstrates the leader election recipe where multiple nodes compete, and only one becomes leader.
"""

import sys
import time
import logging
import uuid
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import ConnectionLossException, SessionExpiredException

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s'
)
logger = logging.getLogger(__name__)

# Node configuration
NODE_ID = str(uuid.uuid4())[:8]
ELECTION_PATH = "/elections/worker-pool"
HOSTS = "localhost:2181,localhost:2182,localhost:2183"

def run_leader_tasks():
    """Tasks executed only by the leader node."""
    logger.info(f"=== [LEADER ACTIVE] Node {NODE_ID} is executing leader tasks ===")
    while True:
        logger.info(f"Leader {NODE_ID} processing cluster tasks...")
        time.sleep(5)

def main():
    logger.info(f"Starting candidate node {NODE_ID} connecting to ZooKeeper at {HOSTS}...")
    
    # Initialize Kazoo client
    zk = KazooClient(hosts=HOSTS, timeout=10.0)
    
    # Connection state listener to handle sessions dropping/reconnecting
    @zk.add_listener
    def connection_listener(state):
        if state == KazooState.LOST:
            logger.warning(f"Session LOST for node {NODE_ID}! Relinquishing leadership or stopping...")
        elif state == KazooState.SUSPENDED:
            logger.warning(f"Session SUSPENDED for node {NODE_ID}! Quorum might be lost...")
        else:
            logger.info(f"Session CONNECTED for node {NODE_ID}.")

    try:
        zk.start()
    except Exception as e:
        logger.error(f"Failed to connect to ZooKeeper ensemble: {e}")
        sys.exit(1)
        
    logger.info(f"Candidate {NODE_ID} successfully joined ensemble. Participating in election...")
    
    # Create election object
    election = zk.Election(ELECTION_PATH, NODE_ID)
    
    try:
        # Contend for leadership. This call blocks until this candidate wins.
        # Once it wins, the run method executes the target function.
        election.run(run_leader_tasks)
    except (ConnectionLossException, SessionExpiredException):
        logger.error(f"ZooKeeper session aborted during election contention. Node {NODE_ID} exiting.")
    except KeyboardInterrupt:
        logger.info(f"Candidate {NODE_ID} received interrupt. Withdrawing from election...")
    finally:
        zk.stop()
        zk.close()
        logger.info(f"Candidate {NODE_ID} disconnected.")

if __name__ == "__main__":
    main()
