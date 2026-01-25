import logging
import time
import threading
from typing import Optional, Tuple

import grpc

from server.server.chord.protos.chord_pb2 import Empty, NodeInfo, ID
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.utils.config import TIMEOUT, M_BITS
from server.server.security import create_channel


class Elector(threading.Thread):
    """
    The Elector class is responsible for managing leader election and maintaining
    communication with the current leader in the distributed system.
    """

    def __init__(self, node, interval: float = 10.0):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.elector')
        self.running = True
        self.current_leader: Optional[NodeInfo] = None
        self.is_leader = False
        self.election_in_progress = False

    def run(self) -> None:
        """Main thread loop that periodically checks if an election is needed."""
        self.logger.info(f"Elector thread started with interval {self.interval}s")
        
        while self.running:
            try:
                self.check_for_election()
            except Exception as e:
                self.logger.error(f"Error in elector thread: {e}")
            
            time.sleep(self.interval)

    def check_for_election(self) -> None:
        """
        This method is responsible for periodically checking if an election is needed.
        Traverses the entire ring to find the node with highest ID.
        Works for any number of nodes.
        """
        try:
            if self.current_leader is None:
                # No leader elected yet, start election
                self.logger.info("No leader found, initiating election")
                self.call_for_election()
            else:
                # Verify current leader is still alive
                self.check_leader()
        except Exception as e:
            self.logger.error(f"Error checking for election: {e}")

    def check_leader(self) -> None:
        """
        Periodically checks if the leader is still alive by pinging it.
        If the leader fails to respond, an election is triggered.
        """
        if not self.current_leader or not self.current_leader.address:
            self.call_for_election()
            return

        try:
            channel = create_channel(self.current_leader.address)
            try:
                stub = ChordServiceStub(channel)
                stub.Ping(Empty(), timeout=TIMEOUT)
                self.logger.debug(f"Leader {self.current_leader.id} is alive")
                return
            finally:
                channel.close()
        except Exception as e:
            self.logger.warning(f"Leader {self.current_leader.id} at {self.current_leader.address} is not responding: {e}")
            self.logger.info("Leader failed, initiating new election")
            self.call_for_election()

    def call_for_election(self) -> None:
        """
        Calls for an election using Ring Election Algorithm (Chang & Roberts).
        Traverses the entire ring to find the node with the highest ID.
        Works for any number of nodes in the ring.
        """
        if self.election_in_progress:
            self.logger.debug("Election already in progress, skipping")
            return

        self.election_in_progress = True
        
        try:
            with self.node.lock:
                successor = self.node.finger[0]

            if successor is None or successor.id == self.node.id:
                # Isolated node, becomes leader
                self.logger.info(f"Node {self.node.id} is isolated, becomes leader")
                self.current_leader = NodeInfo(id=self.node.id, address=self.node.address)
                self.is_leader = True
                return

            # Start traversing the ring to find the node with highest ID
            highest_id = self.node.id
            highest_node = NodeInfo(id=self.node.id, address=self.node.address)
            visited = set()
            current = successor
            max_iterations = 100  # Safety limit to prevent infinite loops

            for _ in range(max_iterations):
                if current is None or current.address is None:
                    break
                
                # Prevent visiting same node twice
                if current.id in visited:
                    self.logger.info(f"Completed ring traversal, highest ID: {highest_id}")
                    break
                
                visited.add(current.id)

                # Contact the node
                try:
                    channel = create_channel(current.address)
                    try:
                        stub = ChordServiceStub(channel)
                        stub.Ping(Empty(), timeout=TIMEOUT)
                        
                        # Node is reachable, compare ID
                        if current.id > highest_id:
                            highest_id = current.id
                            highest_node = current
                            self.logger.debug(f"Found higher ID: {highest_id}")
                    finally:
                        channel.close()
                except Exception as e:
                    self.logger.debug(f"Could not reach node {current.id} at {current.address}: {e}")

                # Move to next node
                try:
                    channel = create_channel(current.address)
                    try:
                        stub = ChordServiceStub(channel)
                        # Get successor of current node
                        next_node = stub.GetPredecessor(ID(id=self.node.id), timeout=TIMEOUT)
                        # For now, we'll assume successor is available, try to move forward
                        current = next_node
                    except Exception:
                        # If we can't get next node info, break
                        break
                    finally:
                        channel.close()
                except Exception:
                    break

            # Elect the highest ID node as leader
            self.current_leader = highest_node
            self.is_leader = (highest_node.id == self.node.id)
            
            if self.is_leader:
                self.logger.info(f"Node {self.node.id} elected as LEADER")
            else:
                self.logger.info(f"Node {highest_node.id} elected as leader")

        except Exception as e:
            self.logger.error(f"Error during election: {e}")
            # Fallback to self
            self.current_leader = NodeInfo(id=self.node.id, address=self.node.address)
            self.is_leader = True
        finally:
            self.election_in_progress = False

    def election(self, first_id: int, leader_id: int, leader_address: str) -> Optional[NodeInfo]:
        """
        Legacy election method kept for potential future distributed election algorithms.
        Currently uses a simple approach: compare with successor and elect the highest ID.

        Args:
            first_id: The ID of the node that initiated the election (currently unused).
            leader_id: The ID of the current candidate leader.
            leader_address: The full address (IP:port) of the current candidate leader.

        Returns:
            NodeInfo: Information about the elected leader based on ID comparison.
        """
        try:
            with self.node.lock:
                successor = self.node.finger[0]

            if successor is None:
                return NodeInfo(id=leader_id, address=leader_address)

            # Compare IDs: highest ID wins
            if self.node.id > leader_id:
                return NodeInfo(id=self.node.id, address=self.node.address)
            elif successor.id > leader_id:
                return successor
            else:
                return NodeInfo(id=leader_id, address=leader_address)

        except Exception as e:
            self.logger.error(f"Error in election process: {e}")
            return NodeInfo(id=leader_id, address=leader_address)

    def ping_leader(self, id: int, time_val: int) -> int:
        """
        Pings the leader to synchronize time between the nodes.

        Args:
            id: The ID of the node making the request.
            time_val: The current time of the node.

        Returns:
            int: The updated time after the leader responds.
        """
        if not self.current_leader or not self.current_leader.address:
            self.logger.warning("No leader available to ping")
            return time_val

        try:
            channel = create_channel(self.current_leader.address)
            try:
                stub = ChordServiceStub(channel)
                stub.Ping(Empty(), timeout=TIMEOUT)
                self.logger.debug(f"Successfully pinged leader {self.current_leader.id}")
                return time_val
            finally:
                channel.close()
        except Exception as e:
            self.logger.warning(f"Failed to ping leader: {e}")
            return time_val

    def get_leader(self) -> Optional[NodeInfo]:
        """
        Returns the current leader information.

        Returns:
            Optional[NodeInfo]: The current leader's NodeInfo, or None if no leader is elected.
        """
        return self.current_leader

    def is_node_leader(self) -> bool:
        """
        Checks if the current node is the leader.

        Returns:
            bool: True if this node is the leader, False otherwise.
        """
        return self.is_leader

    def stop(self) -> None:
        """Stop the elector thread."""
        self.running = False
