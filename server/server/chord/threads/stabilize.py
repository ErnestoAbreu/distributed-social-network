import logging
import time
import threading

import grpc

from server.server.chord.protos.chord_pb2 import Empty, NodeInfo
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.utils.config import TIMEOUT
from server.server.chord.utils.utils import is_in_interval

class Stabilizer(threading.Thread):
    def __init__(self, node, interval):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.stabilize')

    def run(self):
        """Periodically verify successor and update predecessor"""
        while True:
            time.sleep(self.interval)
            try:
                with self.node.lock:
                    succ = self.node.finger[0]

                if not succ or succ.address == self.node.address:
                    continue
                
                try:
                    # Ask successor for its predecessor
                    channel = grpc.insecure_channel(succ.address)
                    stub = ChordServiceStub(channel)
                    succ_pred = stub.GetPredecessor(Empty(), timeout=TIMEOUT)
                    channel.close()
                    
                    # If successor's predecessor is between us and successor, it should be our successor
                    if succ_pred and succ_pred.address and succ_pred.address != self.node.address:
                        if is_in_interval(succ_pred.id, self.node.id, succ.id):
                            with self.node.lock:
                                self.node.finger[0] = succ_pred
                            self.logger.info(f"Updated successor to {succ_pred.address}")
                except Exception as e:
                    self.logger.warning(f"Stabilize failed: {e}")
                    continue
                
                # Notify successor that we might be its predecessor
                try:
                    with self.node.lock:
                        succ = self.node.finger[0]
                    channel = grpc.insecure_channel(succ.address)
                    stub = ChordServiceStub(channel)
                    stub.UpdatePredecessor(NodeInfo(id=self.node.id, address=self.node.address), timeout=TIMEOUT)
                    channel.close()
                except Exception as e:
                    self.logger.warning(f"Notify successor failed: {e}")
            except Exception as e:
                self.logger.error(f"Stabilize loop error: {e}")

            
                