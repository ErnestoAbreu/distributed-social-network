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
            self.logger.debug(f"could not log finger table: {e}")

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

                channel = grpc.insecure_channel(successor.address)
                stub = ChordServiceStub(channel)
                response = stub.GetPredecessor(Empty(), timeout=TIMEOUT)
                channel.close()

                with self.node.lock:
                    pred = self.node.predecessor

                # Check if we have a better successor
                if response.address and is_in_interval(response.id, self.node.id, successor.id):
                    with self.node.lock:
                        self.node.finger[0] = response
                        successor = response

                # Notify successor about ourselves
                with self.node.lock:
                    my_info = NodeInfo(id=self.node.id, address=self.node.address)

                channel = grpc.insecure_channel(successor.address)
                stub = ChordServiceStub(channel)
                stub.UpdatePredecessor(my_info, timeout=TIMEOUT)
                channel.close()

                # Recompute finger table entries because network changed
                try:
                    for i in range(M_BITS):
                        start = (self.node.id + (1 << i)) % (1 << M_BITS)
                        try:
                            succ_i = self.node.find_successor(start)
                            with self.node.lock:
                                self.node.finger[i] = succ_i
                        except Exception as ie:
                            self.logger.debug(f"failed to update finger {i}: {ie}")
                except Exception as e:
                    self.logger.warning(f"updating finger table failed: {e}")
                    
                # Log the current finger table for debugging/visibility (throttled to 60s)
                try:
                    now = time.time()
                    if now - self.last_log_time >= 60:
                        self.log_finger_table()
                        self.last_log_time = now
                except Exception as e:
                    self.logger.debug(f"could not evaluate log throttle: {e}")

            except Exception as e:
                self.logger.error(f"Stabilization loop error: {e}")


            
                