import logging
import time
import threading

import grpc

from server.server.chord.protos.chord_pb2 import Empty, NodeInfo
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.utils.config import TIMEOUT, M_BITS
from server.server.chord.utils.utils import is_in_interval

class Stabilizer(threading.Thread):
    def __init__(self, node, interval):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.stabilize')
        self.last_log_time = 0

    def _log_finger_table(self):
        try:
            now = time.time()
            if now - self.last_log_time >= 30:
                with self.node.lock:
                    fingers_snapshot = list(self.node.finger)

                entries = []
                for idx, n in enumerate(fingers_snapshot):
                    if n:
                        entries.append(f"[{idx}]=id:{n.id}@{n.address}")
                    else:
                        entries.append(f"[{idx}]=None")

                self.logger.info("Finger table: %s", ", ".join(entries))
                self.logger.info(f"Predecessor: id:{getattr(self.node.predecessor,'id',None)}@{getattr(self.node.predecessor,'address',None)}")
                self.last_log_time = now
        except Exception as e:
            self.logger.info(f"could not log finger table: {e}")

    def _ping_node(self, node):
        """Check if a node is reachable"""
        if not node or not node.address:
            return False
        try:
            channel = grpc.insecure_channel(node.address)
            try:
                stub = ChordServiceStub(channel)
                stub.Ping(Empty(), timeout=TIMEOUT)
                return True
            finally:
                channel.close()
        except Exception as e:
            self.logger.warning(f"Node {node.address} unreachable: {e}")
            return False

    def _find_next_alive_successor(self):
        """Find the next alive successor in the finger table"""
        with self.node.lock:
            fingers_copy = list(self.node.finger)

        for finger in fingers_copy:
            if finger and finger.address != self.node.address:
                if self._ping_node(finger):
                    return finger
                else:
                    finger = None  # Mark as dead
                
        self.logger.warning(f"Node is isolated {self.node.address}, no alive successors found")
                
        # If not alive successor, return self node itself
        return NodeInfo(id=self.node.id, address=self.node.address)
    
    def _fix_successor(self) -> bool:
        """Check and fix the successor node"""
        with self.node.lock:
            successor = self.node.finger[0]

        if not successor:
            return False
        
         # Try getting successor's predecessor
        try:
            channel = grpc.insecure_channel(successor.address)
            try:
                stub = ChordServiceStub(channel)
                middle_node = stub.GetPredecessor(Empty(), timeout=TIMEOUT)
            finally:
                channel.close()
        except Exception as e:
            self.logger.warning(f"Failed to get predecessor from {successor.address}: {e}")
            return False

        # Validate middle_node before using it
        if not middle_node or not middle_node.address:
            self.logger.debug(f"Invalid predecessor response from {successor.address}")
            return False

        # Check if middle_node is our new successor
        if is_in_interval(middle_node.id, self.node.id, successor.id):
            # Check if the reported predecessor is alive
            if self._ping_node(middle_node):
                with self.node.lock:
                    self.node.finger[0] = middle_node
                    successor = middle_node
                    return True
            else:
                self.logger.warning(f"Reported predecessor {middle_node.address} is dead, ignoring")
            
        return False
    
    def _notify_successor(self):
        with self.node.lock:
            successor = self.node.finger[0]

        if not successor:
            return
        
        try:
            channel = grpc.insecure_channel(successor.address)
            try:
                stub = ChordServiceStub(channel)
                stub.UpdatePredecessor(NodeInfo(id=self.node.id, address=self.node.address), timeout=TIMEOUT)
            finally:
                channel.close()
        except Exception as e:
            self.logger.warning(f"Failed to notify successor {successor.address}: {e}")

    def _fix_finger_table(self):
        """Update finger table with error handling"""
        try:
            for i in range(M_BITS):
                start = (self.node.id + (1 << i)) % (1 << M_BITS)
                try:
                    succ_i = self.node.find_successor(start)
                    if succ_i:
                        with self.node.lock:
                            self.node.finger[i] = succ_i
                except Exception as ie:
                    self.logger.debug(f"failed to update finger {i}: {ie}")
        except Exception as e:
            self.logger.warning(f"updating finger table failed: {e}")

    def run(self):
        """Periodically stabilize finger table and predecessor"""
        while True:
            time.sleep(self.interval)
            try:
                # Get successor
                with self.node.lock:
                    successor = self.node.finger[0]

                if not successor:
                    continue

                # Check if successor is alive before trying to contact it
                network_changed = False
                if not self._ping_node(successor):
                    self.logger.warning(f"Successor {successor.address} is dead, finding new successor")
                    new_successor = self._find_next_alive_successor()
                    with self.node.lock:
                        self.node.finger[0] = new_successor
                        successor = new_successor
                    network_changed = True

                    # If we are our own successor, skip stabilization
                    if successor.address == self.node.address:
                        self.logger.info("Single node in ring, skipping stabilization")
                        continue
                
                # Fix successor if needed
                network_changed = network_changed or self._fix_successor() 

                # Notify successor about ourselves        
                self._notify_successor()                

                if not network_changed:
                    continue

                # Recompute finger table entries because network changed
                self._fix_finger_table()
                    
                # Log the current finger table for debugging/visibility (throttled to 30s)
                self._log_finger_table()
                
            except Exception as e:
                self.logger.error(f"Stabilization loop error: {e}")