import logging
import time
import threading
import grpc

from server.server.chord.protos.chord_pb2 import Empty, KeyValue, Key, ID
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.utils.config import TIMEOUT, REPLICATION_K, REPLICATION_INTERVAL
from server.server.chord.utils.hashing import hash_key

class Replicator(threading.Thread):
    def __init__(self, node, interval = REPLICATION_INTERVAL):
        super().__init__(daemon=True)
        self.node = node
        self.interval = interval
        self.logger = logging.getLogger('socialnet.server.chord.replicator')

    def get_successor_list(self, count=REPLICATION_K):
        """Get a list of the next 'count' successor nodes for replication"""
        successors = []

        with self.node.lock:
            current = self.node.finger[0]

        if not current or current.address == self.node.address:
            return successors
        
        # Start with inmediate successor
        successors.append(current)

        # Get additional successors by querying each node for its successor
        for _ in range(count - 1):
            try:
                if not current or current.address == self.node.address:
                    break

                channel = grpc.insecure_channel(current.address)
                stub = ChordServiceStub(channel)
                next_node = stub.GetSuccessor(ID(id=current.id), timeout=TIMEOUT)
                channel.close()

                if next_node and next_node.address != self.node.address:
                    # Avoid duplicates
                    if not any(s.address == next_node.address for s in successors):
                        successors.append(next_node)
                    current = next_node
                else:
                    break

            except Exception as e:
                self.logger.warning(f"Failed to get successor from {current.address}: {e}")
                break
        
        return successors
    
    def ping_node(self, node):
        """Check if a node is alive with a short timeout"""
        if not node or not node.address:
            return False
        try:
            channel = grpc.insecure_channel(node.address)
            stub = ChordServiceStub(channel)
            stub.Ping(Empty(), timeout=TIMEOUT)
            channel.close()
            return True
        except Exception as e:
            self.logger.debug(f"Node {node.address} unreachable: {e}")
            return False

    def replicate_data(self):
        """Replica all local data to successor nodes"""
        successors = self.get_successor_list(REPLICATION_K - 1)

        if not successors:
            self.logger.debug("No successors available for replication")
            return
        
        # Filter out dead nodes before attempting replication
        alive_successors = [s for s in successors if self.ping_node(s)]
        
        if not alive_successors:
            self.logger.debug("No alive successors available for replication")
            return
        
        # Get all items from storage
        items = self.node.storage.items()

        if not items:
            self.logger.debug("No data to replicate")
            return
        
        self.logger.info(f"Replicating {len(items)} items to {len(alive_successors)} alive successors")

        # Replicate each item to all alive successors
        for key, value in items.items():
            for successor in alive_successors:
                try:
                    self._replicate_item(successor, key, value)
                except Exception as e:
                    self.logger.error(f"Failed to replicate key {key} to {successor.address}: {e}")

    def _replicate_item(self, node, key, value):
        """Replicate a single key-value pair to a given node"""
        try:
            channel = grpc.insecure_channel(node.address)
            stub = ChordServiceStub(channel)
            stub.Put(KeyValue(key=key, value=value), timeout=TIMEOUT)
            channel.close()
            self.logger.debug(f"Replicated key {key} to {node.address}")
        except Exception as e:
            self.logger.warning(f"Failed to replicate key {key} to {node.address}: {e}")
            raise

    def resolve_replicas(self):
        """Check and resolve replicas for consistency"""
        items = self.node.storage.items()
        keys_to_transfer = []
        for key in items.keys():
            # Hash the key to determine its proper owner
            key_hash = hash_key(key, self.node.id.bit_length())

            # Find the successor responsible for this key
            try:
                responsible_node = self.node.find_successor(key_hash)

                # If we're not responsible and it's not a replica we should keep
                if responsible_node.address != self.node.address:
                    if not self._should_keep_replica(key_hash):
                        keys_to_transfer.append((key, responsible_node))

            except Exception as e:
                self.logger.error(f"Failed to resolve replica for key {key}: {e}")

        # Transfer keys that don't belong to us
        for key, target_node in keys_to_transfer:
            try:
                value = self.node.storage.get(key)
                if value:
                    self._replicate_item(target_node, key, value)
                    self.node.storage.delete(key)
                    self.logger.info(f"Transferred key {key} to {target_node.address}")
            except Exception as e:
                self.logger.error(f"Failed to transfer key {key} to {target_node.address}: {e}")
    
    def _should_keep_replica(self, key_hash):
        """Determine if this node should keep the replica for the given key hash"""
        try:
            # Find the node responsible for this key
            responsible_node = self.node.find_successor(key_hash)

            # If we are the responsible node, we should keep it
            if responsible_node.address == self.node.address:
                return True
            
            # Get our successors
            our_successors = self.get_successor_list(REPLICATION_K)

            predecessor = self.node.predecessor
            if not predecessor:
                return False
            
            # Check if responsible_node is in our "predecessor chain" within REPLICATION_K hops
            current_check = self.node.predecessor
            for distance in range(1, REPLICATION_K):
                if not current_check:
                    break
                if current_check.id == responsible_node.id:
                    return True
                # Move back one more step
                try:
                    channel = grpc.insecure_channel(current_check.address)
                    stub = ChordServiceStub(channel)
                    prev_pred = stub.GetPredecessor(Empty(), timeout=TIMEOUT)
                    channel.close()
                    current_check = prev_pred if prev_pred else None
                except Exception as e:
                    break
            
            return False
        except Exception as e:
            self.logger.warning(f"Error checking if should keep replica for key hash {key_hash}: {e}")
            return False

    def fetch_replicas_from_successor(self):
        """Fetch replicas from immediate successor"""
        with self.node.lock:
            successor = self.node.finger[0]

        if not successor or successor.address == self.node.address:
            self.logger.debug("No valid successor to fetch replicas from")
            return
        
        try:
            channel = grpc.insecure_channel(successor.address)
            stub = ChordServiceStub(channel)
            response = stub.GetAllKeys(Empty(), timeout=TIMEOUT)
            channel.close()

            if response and response.items:
                self.logger.info(f"Fetching {len(response.items)} replicas from successor {successor.address}")
                for kv in response.items:
                    self.node.storage.put(kv.key, kv.value)
                self.logger.info(f"Successfully fetched replicas from {successor.address}")

        except Exception as e:
            self.logger.error(f"Failed to fetch replicas from {successor.address}: {e}")

    def run(self):
        """Main loop for periodic replication and resolution"""
        self.logger.info("Replicator thread started")

        # Intitial delay to let the node stabilize
        time.sleep(self.interval)

        while True:
            try:
                self.replicate_data()

                if int(time.time()) % (self.interval * 5) == 0:
                    self.resolve_replicas()
            
            except Exception as e:
                self.logger.error(f"Error during replication cycle: {e}")
            
            time.sleep(self.interval)