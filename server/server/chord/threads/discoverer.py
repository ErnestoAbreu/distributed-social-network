import logging
import os
import socket
import time
import grpc
import threading

from server.server.chord.utils.config import TIMEOUT, DISCOVERY_INTERVAL
from server.server.config import DEFAULT_PORT
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.protos.chord_pb2 import Empty, ID, NodeInfo

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
        """
        try:
            with self.node.lock:
                successor = self.node.finger[0] if self.node.finger[0] else None
                predecessor = self.node.predecessor
            
            leader_info = ""
            if self.node.elector and self.node.elector.current_leader:
                leader = self.node.elector.current_leader
                is_leader = self.node.elector.is_leader
                leader_info = f" | Leader: {leader.id}@{leader.address}" + (" (THIS NODE)" if is_leader else "")
            
            self.logger.info("=== Network Status ===")
            self.logger.info(f"Current Node: {self.node.id}@{self.node.address}")
            
            if successor:
                self.logger.info(f"Successor: {successor.id}@{successor.address}")
            else:
                self.logger.info("Successor: None")
            
            if predecessor:
                self.logger.info(f"Predecessor: {predecessor.id}@{predecessor.address}")
            else:
                self.logger.info("Predecessor: None")
            
            is_isolated = (successor and successor.address == self.node.address) or not successor
            self.logger.info(f"Status: {'Isolated (Leader)' if is_isolated else 'Connected to ring'}{leader_info}")
            
        except Exception as e:
            self.logger.error(f"Error logging network status: {e}")

    def discover_nodes(self) -> list[str]:
        """
        Discover existing Chord nodes using Docker DNS.
        
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
                if candidate_addr != self.node.address:
                    existing_nodes.append(candidate_addr)
            
            self.logger.info(f'Discovered {len(existing_nodes)} potential nodes: {existing_nodes}')
        except Exception as e:
            self.logger.debug(f'DNS discovery failed: {e}')
        
        return existing_nodes

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
                channel = grpc.insecure_channel(candidate_addr)
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
        Monitors topology changes but lets elector handle election timing.
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
                else:
                    consecutive_unchanged += 1
                
                if is_isolated:
                    self.logger.info("Node is isolated. Attempting to discover and join ring...")
                    self.create_ring_or_join()
                else:
                    # Node is connected, just log status periodically to avoid spam
                    if consecutive_unchanged % 5 == 0:  # Log every 5 checks (50 seconds)
                        self.logger.debug(f"Node is connected. Successor: {successor.id}@{successor.address}")
                        self.log_network_status()
                
                # Wait before next check
                time.sleep(self.interval)
                
            except Exception as e:
                self.logger.error(f"Error in discovery loop: {e}")
                time.sleep(self.interval)
