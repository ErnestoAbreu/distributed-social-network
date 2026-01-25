import logging
import os
import socket
import time
import threading

from server.server.security import create_channel
from server.server.chord.utils.utils import is_in_interval
from server.server.chord.utils.config import TIMEOUT, DISCOVERY_INTERVAL
from server.server.chord.utils.cache import load_node_cache, add_to_node_cache
from server.server.config import DEFAULT_PORT
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.protos.chord_pb2 import Empty, NodeInfo

logger = logging.getLogger('socialnet.server.chord.threads.discoverer')


class Discoverer(threading.Thread):
    """
    A class to handle the discovery and joining of a Chord ring. The class provides
    methods for discovering an existing chord ring, joining it, or creating a new ring.
    """

    def __init__(self, node, interval=DISCOVERY_INTERVAL):
        """
        Initialize the Discoverer with a reference to a ChordNode.
        
        Args:
            node: The Chord node instance to manage.
            interval (int): Time in seconds between discovery checks (default: DISCOVERY_INTERVAL).
        """
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.discoverer')

    def log_network_status(self):
        """
        Logs the current network status, including active servers, leader, and their connections.
        Shows live updates for successor and predecessor.
        """
        try:
            self._refresh_topology_info()
            
            with self.node.lock:
                successor = self.node.finger[0] if self.node.finger[0] else None
                predecessor = self.node.predecessor
            
            if successor:
                successor_info = f"{successor.id}@{successor.address}"
                is_self_successor = successor.address == self.node.address
                successor_str = f"Successor: {successor_info}" + (" (SELF)" if is_self_successor else "")
            else:
                successor_str = "Successor: None"
            
            if predecessor:
                predecessor_info = f"{predecessor.id}@{predecessor.address}"
                predecessor_str = f"Predecessor: {predecessor_info}"
            else:
                predecessor_str = "Predecessor: None"
            
            leader_info = ""
            if self.node.elector and self.node.elector.current_leader:
                leader = self.node.elector.current_leader
                is_leader = self.node.elector.is_leader
                leader_status = " (THIS NODE)" if is_leader else ""
                leader_info = f" | Leader: {leader.id}@{leader.address}{leader_status}"
            
            # Determine network status
            is_isolated = (successor and successor.address == self.node.address) or not successor
            network_status = "Isolated (Leader)" if is_isolated else "Connected to ring"
            
            # Log all status information
            self.logger.info("=" * 60)
            self.logger.info(f"NETWORK STATUS")
            self.logger.info(f"  Node: {self.node.id}@{self.node.address}")
            self.logger.info(f"  {successor_str}")
            self.logger.info(f"  {predecessor_str}")
            self.logger.info(f"  Status: {network_status}{leader_info}")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Error logging network status: {e}")

    def _refresh_topology_info(self):
        """
        Refreshes successor and predecessor information by querying the current successor.
        This ensures the logged info is up-to-date.
        """
        try:
            with self.node.lock:
                successor = self.node.finger[0]
            
            if not successor or successor.address == self.node.address:
                # Node is isolated, no refresh needed
                return
            
            # Query successor's predecessor to check for a better successor
            try:
                channel = create_channel(successor.address, options=[('grpc.keepalive_time_ms', 5000)])
                try:
                    stub = ChordServiceStub(channel)
                    middle_node = stub.GetPredecessor(Empty(), timeout=TIMEOUT)
                    
                    # If middle_node is alive and between us and current successor, update successor
                    if middle_node and middle_node.address and middle_node.address != self.node.address:
                        if is_in_interval(middle_node.id, self.node.id, successor.id):
                            with self.node.lock:
                                old_successor = self.node.finger[0]
                                self.node.finger[0] = middle_node
                            if old_successor.address != middle_node.address:
                                self.logger.debug(f"Updated successor from {old_successor.address} to {middle_node.address}")
                finally:
                    channel.close()
            except Exception as e:
                self.logger.debug(f"Failed to refresh successor info: {e}")
            
            # Try to update predecessor from successor
            try:
                with self.node.lock:
                    current_successor = self.node.finger[0]
                
                if current_successor and current_successor.address != self.node.address:
                    channel = create_channel(current_successor.address, options=[('grpc.keepalive_time_ms', 5000)])
                    try:
                        stub = ChordServiceStub(channel)
                        # Request successor to check its predecessor (which should be us or closer to us)
                        stub.UpdatePredecessor(
                            NodeInfo(id=self.node.id, address=self.node.address), 
                            timeout=TIMEOUT
                        )
                    finally:
                        channel.close()
            except Exception as e:
                self.logger.debug(f"Failed to refresh predecessor info: {e}")
                
        except Exception as e:
            self.logger.debug(f"Error refreshing topology info: {e}")

    def discover_nodes(self) -> list[str]:
        """
        Discover existing Chord nodes using Docker DNS.
        Falls back to cached nodes if DNS discovery fails.
        
        Returns:
            list[str]: A list of discovered node addresses.
        """
        existing_nodes = []
        
        try:
            network_alias = os.getenv('NETWORK_ALIAS', 'socialnet_server')
            self.logger.debug(f'Discovering nodes via DNS lookup: {network_alias}')
            
            _, _, ip_list = socket.gethostbyname_ex(network_alias)
            
            for ip in ip_list:
                candidate_addr = f'{ip}:{DEFAULT_PORT}'
                add_to_node_cache(candidate_addr)
                if candidate_addr != self.node.address:
                    existing_nodes.append(candidate_addr)
            
            self.logger.info(f'Discovered {len(existing_nodes)} potential nodes: {existing_nodes}')

            return existing_nodes
            
        except Exception as e:
            self.logger.debug(f'DNS discovery failed: {e}')
            
            # Try cached nodes as fallback
            self.logger.info('DNS failed, attempting to use cached nodes')
            cached_nodes = load_node_cache()

            self.logger.info(f'Loaded {len(cached_nodes)} cached nodes')
            if cached_nodes:
                # Filter out self
                available_nodes = [addr for addr in cached_nodes if addr != self.node.address]
                self.logger.info(f'Found {len(available_nodes)} cached nodes: {available_nodes}')
                return available_nodes
            else:
                self.logger.warning('No cached nodes available')
                return []

    def join(self, candidate_nodes: list[str]) -> bool:
        """
        Joins the chord ring by connecting to a specified node.
        Allows time for topology to stabilize before triggering election.
        
        Args:
            candidate_nodes (list[str]): List of node addresses to attempt joining through.
            
        Returns:
            bool: True if successfully joined, False otherwise.
        """
        for candidate_addr in candidate_nodes:
            try:
                self.logger.info(f'Attempting to join ring via {candidate_addr}...')
                
                # Test if node is reachable
                channel = create_channel(candidate_addr)
                stub = ChordServiceStub(channel)
                stub.Ping(Empty(), timeout=TIMEOUT)
                channel.close()
                
                # Join through this node
                self.node.join(NodeInfo(address=candidate_addr))
                self.logger.info(f'Successfully joined Chord ring via {candidate_addr}')
                
                # Wait for topology to stabilize before election
                # This gives the Stabilizer thread time to update finger tables
                self.logger.info('Waiting for ring topology to stabilize before election...')
                time.sleep(3)
                
                # Trigger election after topology stabilization
                if self.node.elector:
                    self.logger.info('Topology stabilized, triggering leader election')
                    self.node.elector.call_for_election()
                else:
                    self.logger.warning('Elector not initialized yet')
                
                self.log_network_status()
                return True
                
            except Exception as e:
                self.logger.debug(f'Failed to join via {candidate_addr}: {e}')
                continue
        
        return False

    def create_ring(self):
        """
        Creates a new chord ring by initializing the node's predecessor, successor, and leader.
        This is used when the node is the first in the ring.
        """
        try:
            self.logger.info("Creating new Chord ring as the first node...")
            
            # Initialize self as the successor (single node ring)
            self_node = NodeInfo(id=self.node.id, address=self.node.address)
            
            with self.node.lock:
                self.node.finger[0] = self_node
                self.node.predecessor = None
            
            # Initialize elector with this node as leader (only node in ring)
            if self.node.elector:
                self.node.elector.current_leader = self_node
                self.node.elector.is_leader = True
                self.logger.info(f"Elector initialized: Node {self.node.id} is the leader")
            else:
                self.logger.warning("Elector not available yet")
            
            self.logger.info(f"New Chord ring created. Node {self.node.id}@{self.node.address} is the only node.")
            self.log_network_status()
            
        except Exception as e:
            self.logger.error(f"Error creating new ring: {e}")

    def create_ring_or_join(self) -> bool:
        """
        Attempts to either join an existing chord ring or create a new ring if none is discovered.
        
        Returns:
            bool: True if successfully joined or created a ring, False otherwise.
        """
        try:
            self.logger.info("Attempting to discover and join existing Chord ring...")
            
            # Discover existing nodes
            candidate_nodes = self.discover_nodes()
            
            if candidate_nodes:
                # Try to join through discovered nodes
                if self.join(candidate_nodes):
                    return True
                else:
                    self.logger.warning("Failed to join through discovered nodes. Creating new ring.")
                    self.create_ring()
                    return True
            else:
                self.logger.info("No existing nodes discovered. Creating new ring.")
                self.create_ring()
                return True
                
        except Exception as e:
            self.logger.error(f"Error in create_ring_or_join: {e}")
            return False

    def _update_leader_status(self):
        """
        Updates the leader status in the elector based on current ring topology.
        Called when ring topology changes to ensure consistency.
        """
        try:
            if not self.node.elector:
                self.logger.debug("Elector not initialized, skipping leader status update")
                return
            
            with self.node.lock:
                successor = self.node.finger[0] if self.node.finger[0] else None
            
            # If node is isolated, it should become leader
            is_isolated = (successor and successor.address == self.node.address) or not successor
            if is_isolated:
                self_node = NodeInfo(id=self.node.id, address=self.node.address)
                self.node.elector.current_leader = self_node
                self.node.elector.is_leader = True
                self.logger.info(f"Topology update: Node {self.node.id} is isolated, promoting to leader")
            else:
                # Clear leader state when joining a ring, let elector compute consensus
                self.node.elector.current_leader = None
                self.node.elector.is_leader = False
                self.logger.info(f"Topology update: Topology changed, clearing leader state for re-election")
            
        except Exception as e:
            self.logger.error(f"Error updating leader status: {e}")

    def run(self):
        """
        Main run method for the discovery thread.
        Periodically checks if the node is isolated and attempts to discover or join a ring.
        Monitors topology changes and logs live network status.
        """
        self.logger.info(f"Starting discovery thread with {self.interval}s interval...")
        
        prev_successor = None
        consecutive_unchanged = 0
        
        while True:
            try:
                # Check current status
                with self.node.lock:
                    successor = self.node.finger[0] if self.node.finger[0] else None
                
                # Check if node is isolated (no ring joined)
                is_isolated = (successor and successor.address == self.node.address) or not successor
                
                # Detect topology changes
                if prev_successor != successor:
                    successor_info = f"{successor.id}@{successor.address}" if successor else "None"
                    prev_successor_info = f"{prev_successor.id}@{prev_successor.address}" if prev_successor else "None"
                    self.logger.info(f"Ring topology changed. Previous successor: {prev_successor_info}, New successor: {successor_info}")
                    self._update_leader_status()
                    consecutive_unchanged = 0
                    prev_successor = successor
                    self.log_network_status()
                else:
                    consecutive_unchanged += 1
                
                if is_isolated:
                    self.logger.info("Node is isolated. Attempting to discover and join ring...")
                    self.create_ring_or_join()
                else:
                    # Node is connected, log status periodically for live monitoring
                    if consecutive_unchanged % 5 == 0:  # Log every 5 checks (50 seconds)
                        self.log_network_status()
                
                # Wait before next check
                time.sleep(self.interval)
                
            except Exception as e:
                self.logger.error(f"Error in discovery loop: {e}")
                time.sleep(self.interval)
