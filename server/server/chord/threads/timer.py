import logging
import time
import threading
from typing import List, Optional, Tuple

import grpc

from server.server.chord.protos.chord_pb2 import Empty, NodeInfo
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.utils.config import TIMEOUT, M_BITS, TIMER_INTERVAL, EVENT_TIME


class Timer(threading.Thread):
    """
    Timer class that manages the time for a node in the Chord network and 
    synchronizes the time using the Berkeley algorithm.
    """

    def __init__(self, node, interval: float = TIMER_INTERVAL):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.timer')
        self.running = True
        self.local_time = time.time()

    def run(self) -> None:
        """Main thread loop that periodically synchronizes time using Berkeley algorithm."""
        self.logger.info(f"Timer thread started with interval {self.interval}s")
        
        while self.running:
            try:
                self.update_time()
            except Exception as e:
                self.logger.error(f"Error in timer thread: {e}")
            
            time.sleep(self.interval)

    def update_time(self) -> None:
        """
        Thread that periodically increments the node's local time and updates it in the time dictionary.
        This method implements Berkeley algorithm for time synchronization.
        """
        synchronized_time = self.berkeley_algorithm()
        
        with self.node.lock:
            self.local_time = synchronized_time
            version = int(synchronized_time * 1000)
            self.node.storage.put(EVENT_TIME, str(synchronized_time), version)
        
        self.logger.debug(f"Updated local time to {synchronized_time}")

    def berkeley_algorithm(self) -> int:
        """
        Implements the Berkeley algorithm for time synchronization in distributed nodes.
        returns The average time calculated based on the clocks of the known nodes.
        """
        times: List[float] = [time.time()]
        
        # Get successor nodes for time synchronization
        successor_nodes = self._get_successor_nodes(count=min(3, M_BITS))
        
        # Collect times from successor nodes
        for node in successor_nodes:
            remote_time = self._get_remote_time(node)
            if remote_time is not None:
                times.append(remote_time)
                self.logger.debug(f"Got time {remote_time} from {node.address}")
        
        # Calculate average time
        if times:
            average_time = sum(times) / len(times)
            self.logger.debug(f"Berkeley algorithm: synchronized time = {average_time} (from {len(times)} nodes)")
            return average_time
        else:
            self.logger.warning("Berkeley algorithm: no valid times collected, using local time")
            return time.time()

    def _get_successor_nodes(self, count: int = 3) -> List[NodeInfo]:
        """
        Get a list of successor nodes from the finger table.
        """
        successors: List[NodeInfo] = []
        
        try:
            with self.node.lock:
                # Collect nodes from finger table
                for i in range(min(count, M_BITS)):
                    if self.node.finger[i] and self.node.finger[i].address != self.node.address:
                        successors.append(self.node.finger[i])
                        if len(successors) >= count:
                            break
        except Exception as e:
            self.logger.warning(f"Error getting successor nodes: {e}")
        
        return successors

    def _get_remote_time(self, node: NodeInfo) -> Optional[float]:
        """
        Get time from a remote node via gRPC
        """
        if not node or not node.address:
            return None
        
        try:
            channel = grpc.insecure_channel(node.address)
            try:
                stub = ChordServiceStub(channel)
                response = stub.GetTime(Empty(), timeout=TIMEOUT)
                
                if response and response.timestamp:
                    return float(response.timestamp)
                return None
            finally:
                channel.close()
        except Exception as e:
            self.logger.debug(f"Failed to get time from {node.address}: {e}")
            return None

    def stop(self) -> None:
        """Stop the timer thread."""
        self.running = False
        self.logger.info("Timer thread stopped")
