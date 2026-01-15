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

    def log_finger_table(self):
        try:
            with self.node.lock:
                fingers_snapshot = list(self.node.finger)

            entries = []
            for idx, n in enumerate(fingers_snapshot):
                if n:
                    entries.append(f"[{idx}]=id:{n.id}@{n.address}")
                else:
                    entries.append(f"[{idx}]=None")

            self.logger.info("Finger table: %s", ", ".join(entries))
        except Exception as e:
            self.logger.info(f"could not log finger table: {e}")

    def ping_node(self, node):
        """Check if a node is reachable"""
        if not node or not node.address:
            return False
        try:
            channel = grpc.insecure_channel(node.address)
            stub = ChordServiceStub(channel)
            stub.Ping(Empty(), timeout=TIMEOUT)
            channel.close()
            return True
        except Exception as e:
            self.logger.warning(f"Node {node.address} unreachable: {e}")
            return False

    def find_next_alive_successor(self):
        """Find the next alive successor in the finger table"""
        with self.node.lock:
            fingers_copy = list(self.node.finger)

        for finger in fingers_copy:
            if finger and finger.address != self.node.address:
                if self.ping_node(finger):
                    return finger
                
        # If not alive successor, return self node itself
        return NodeInfo(id=self.node.id, address=self.node.address)

    def run(self):
        """Periodically stabilize finger table and predecessor"""
        while True:
            time.sleep(self.interval)
            try:
                # Get successor's predecessor
                with self.node.lock:
                    successor = self.node.finger[0]

                if not successor:
                    continue

                # Check if successor is alive before trying to contact it
                if not self.ping_node(successor):
                    self.logger.warning(f"Successor {successor.address} is dead, finding new successor")
                    new_successor = self.find_next_alive_successor()
                    with self.node.lock:
                        self.node.finger[0] = new_successor
                        successor = new_successor

                    # If we are our own successor, skip stabilization
                    if successor.address == self.node.address:
                        self.logger.info("Single node in ring, skipping stabilization")
                        continue
                
                # Try getting successor's predecessor
                try:
                    channel = grpc.insecure_channel(successor.address)
                    stub = ChordServiceStub(channel)
                    response = stub.GetPredecessor(Empty(), timeout=TIMEOUT)
                    channel.close()
                except grpc.RpcError as e:
                    self.logger.warning(f"Failed to get predecessor from {successor.address}: {e}")
                    continue

                with self.node.lock:
                    pred = self.node.predecessor

                # Check if we have a better successor
                if response.address and is_in_interval(response.id, self.node.id, successor.id):
                    # Check if the reported predecessor is alive
                    if self.ping_node(response):
                        with self.node.lock:
                            self.node.finger[0] = response
                            successor = response
                    else:
                        self.logger.warning(f"Reported predecessor {response.address} is dead, ignoring")

                # Notify successor about ourselves
                with self.node.lock:
                    my_info = NodeInfo(id=self.node.id, address=self.node.address)
                
                try:
                    channel = grpc.insecure_channel(successor.address)
                    stub = ChordServiceStub(channel)
                    stub.UpdatePredecessor(my_info, timeout=TIMEOUT)
                    channel.close()
                except grpc.RpcError as e:
                    self.logger.warning(f"Failed to notify successor {successor.address}: {e}")
                    continue

                # Recompute finger table entries because network changed
                try:
                    for i in range(M_BITS):
                        start = (self.node.id + (1 << i)) % (1 << M_BITS)
                        try:
                            succ_i = self.node.find_successor(start)
                            with self.node.lock:
                                self.node.finger[i] = succ_i
                        except Exception as ie:
                            self.logger.info(f"failed to update finger {i}: {ie}")
                except Exception as e:
                    self.logger.warning(f"updating finger table failed: {e}")
                    
                # Log the current finger table for debugging/visibility (throttled to 60s)
                try:
                    now = time.time()
                    if now - self.last_log_time >= 60:
                        self.log_finger_table()
                        self.last_log_time = now
                except Exception as e:
                    self.logger.info(f"could not evaluate log throttle: {e}")

            except Exception as e:
                self.logger.error(f"Stabilization loop error: {e}")


            
                