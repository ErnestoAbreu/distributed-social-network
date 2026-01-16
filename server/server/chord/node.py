from datetime import datetime, timezone
import logging
import time
import grpc
import threading
from concurrent import futures

from server.server.chord.threads.stabilize import Stabilizer
from server.server.chord.threads.replicator import Replicator
from server.server.chord.utils.utils import is_in_interval

from .protos.chord_pb2 import ID, Key, NodeInfo, Empty, Value, KeyValue, KeyValueList, Partition, Ack, PartitionResult, TimeStamp
from .protos.chord_pb2_grpc import ChordServiceServicer, ChordServiceStub, add_ChordServiceServicer_to_server

from .utils.hashing import hash_key
from .utils.config import *
from .storage import Storage

logger = logging.getLogger('socialnet.server.chord.node')

class ChordNode(ChordServiceServicer):
    """
    ChordNode is a gRPC service implementation that represents a node in a Chord distributed hash table.
    This class implements the ChordServiceServicer interface and manages:
    - Node identity and routing information using consistent hashing
    - Finger table for efficient node lookup in the Chord ring
    - Storage of key-value pairs with replication support
    - Thread-safe operations using locks
    - Chord protocol maintenance through stabilization and replication
    Attributes:
        address (str): Network address of this node in format 'host:port'
        id (int): Unique identifier for this node derived from hashing the address
        predecessor (NodeInfo): Reference to the predecessor node in the Chord ring
        storage (Storage): Local storage for key-value pairs
        lock (threading.Lock): Synchronization lock for thread-safe operations
        finger (list[NodeInfo]): Finger table with M_BITS entries for routing
        next_finger (int): Index tracking the next finger to be updated during stabilization
        replicator (Replicator): Component managing data replication across nodes
    Methods (RPC Endpoints):
        FindSuccessor(request, context) -> NodeInfo:
            Locates the successor node responsible for a given key ID.
        GetPredecessor(request, context) -> NodeInfo:
            Returns the predecessor node information.
        UpdatePredecessor(request, context) -> Empty:
            Updates the predecessor node and triggers reconciliation if changed.
        Ping(request, context) -> Empty:
            Health check endpoint to verify node availability.
        Get(request, context) -> Value:
            Retrieves the value associated with a given key.
        Put(request, context) -> Empty:
            Stores or updates a key-value pair in local storage.
        Delete(request, context) -> Empty:
            Removes a key-value pair from local storage.
        GetAllKeys(request, context) -> KeyValueList:
            Returns all stored key-value pairs on this node.
        SetPartition(request, context) -> Ack:
            Sets a data partition for replication management.
        ResolveData(request, context) -> PartitionResult:
            Resolves conflicting data versions in replicated partitions.
    Methods (Chord Protocol):
        join(known_node) -> None:
            Joins an existing Chord ring or creates a new one.
        find_successor(key) -> NodeInfo:
            Recursively locates the successor node responsible for a given key.
        closest_preceding_node(key) -> NodeInfo:
            Finds the closest preceding node in the finger table for key lookup.
        ping_node(node) -> bool:
            Verifies if a node is alive by sending a ping request.
        serve() -> None:
            Starts the gRPC server and maintenance threads.
    """

    def __init__(self, address):
        self.address = address
        self.id = hash_key(address, M_BITS)
        self.predecessor = None
        self.storage = Storage()
        self.lock = threading.Lock()

        self.finger = [None] * M_BITS
        self.next_finger = 0

        self.replicator = None


    # ---------------- Chord RPCs ----------------
    def FindSuccessor(self, request: ID, context) -> NodeInfo:
        try:
            return self.find_successor(request.id)
        except Exception as e:
            logger.error(f"finding successor failed: {e}")
            return NodeInfo(id=self.id, address=self.address)


    def GetPredecessor(self, request: Empty, context) -> NodeInfo:
        if self.predecessor:
            return self.predecessor
        return NodeInfo(id=self.id, address=self.address)


    def UpdatePredecessor(self, request: NodeInfo, context) -> Empty:
        try:
            predecessor_changed = False
            with self.lock:
                # Update predecessor if we don't have one, or if the request
                # is between our current predecessor and ourselves.
                if (not self.predecessor) or is_in_interval(request.id, self.predecessor.id, self.id):
                    predecessor_changed = (not self.predecessor) or self.predecessor.id != request.id or self.predecessor.address != request.address

                    self.predecessor = request
                    logger.info(f"UpdatePredecessor: predecessor set to {request.id}@{request.address}")

            # Kick off a reconciliation pass with our predecessor (non-blocking).
            if predecessor_changed and self.replicator and request and request.address and request.address != self.address:
                threading.Thread(target=self.replicator.delegate_to_predecessor, args=(request,), daemon=True,).start()

        except Exception as e:
            logger.error(f"UpdatePredecessor error: {e}")
        return Empty()


    def Ping(self, request: Empty, context) -> Empty:
        return Empty()


    def Get(self, request: Key, context) -> Value:
        val = self.storage.get(request.key)
        return Value(value=val if val else "")


    def Put(self, request: KeyValue, context) -> Empty:
        self.storage.put(request.key, request.value, self.now_version())
        return Empty()


    def Delete(self, request: Key, context) -> Empty:
        self.storage.delete(request.key, self.now_version())
        return Empty()


    def GetAllKeys(self, request: Empty, context) -> KeyValueList:
        try:
            items = self.storage.items()
            kv_list = [KeyValue(key=k, value=v) for k, v in items.items()]
            logger.debug(f"GetAllKeys: returning {len(kv_list)} items")
            return KeyValueList(items=kv_list)
        except Exception as e:
            logger.error(f"GetAllKeys error: {e}")
            return KeyValueList(items=[])


    def SetPartition(self, request: Partition, context) -> Ack:
        try:
            if not self.replicator:
                return Ack(ok=False)

            ok = self.replicator.set_partition(
                dict(request.values),
                dict(request.versions),
                dict(request.removed),
            )
            return Ack(ok=bool(ok))
        except Exception as e:
            logger.error(f"SetPartition error: {e}")
            return Ack(ok=False)


    def ResolveData(self, request: Partition, context) -> PartitionResult:
        try:
            if not self.replicator:
                return PartitionResult(ok=False, partition=Partition())

            res_values, res_versions, res_removed = self.replicator.resolve_data(
                dict(request.values),
                dict(request.versions),
                dict(request.removed),
            )

            return PartitionResult(
                ok=True,
                partition=Partition(values=res_values, versions=res_versions, removed=res_removed),
            )
        except Exception as e:
            logger.error(f"ResolveData error: {e}")
            return PartitionResult(ok=False, partition=Partition())


    def GetTime(self, request: Empty, context) -> TimeStamp:
        with self.lock:
            timestamp = time.time()
            if self.storage.exists(EVENT_TIME):
                stored_time = self.storage.get(EVENT_TIME)
                timestamp = float(stored_time)
                while time.time() < timestamp:
                    time.sleep(0.001)
            
                timestamp = time.time()
                self.storage.put(EVENT_TIME, str(timestamp))

        return TimeStamp(timestamp=str(timestamp))
    

    # ---------------- Chord Logic ----------------
    def join(self, known_node: NodeInfo):
        if known_node:
            logger.info(f"Joining Chord ring via node {known_node.address}")

            channel = grpc.insecure_channel(known_node.address)
            stub = ChordServiceStub(channel)
            successor = stub.FindSuccessor(ID(id=self.id), timeout=TIMEOUT)
            channel.close()

            if successor and successor.address:
                self.finger[0] = successor
                logger.info(f"Successfully joined ring, successor is {successor.address}")
            else:
                logger.error("Getting successor failed")
                logger.info("Creating new Chord ring")
                self.finger[0] = NodeInfo(id=self.id, address=self.address)
        else:
            logger.info("Creating new Chord ring")
            self.finger[0] = NodeInfo(id=self.id, address=self.address)


    def find_successor(self, key: int) -> NodeInfo:
        # read successor atomically
        with self.lock:
            succ = self.finger[0] or NodeInfo(id=self.id, address=self.address)

        # logger.debug(f"find_successor: key={key} my_id={self.id} succ={getattr(succ,'id',None)}@{getattr(succ,'address',None)}")

        # If we are the only node (successor is self), return ourselves
        if succ.address == self.address:
            logger.debug("find_successor: single node ring -> return self")
            return succ
        
        # If id is between us and our successor (handles wrap-around)
        if is_in_interval(key, self.id, succ.id, inclusive_end=True):
            logger.debug(f"find_successor: key {key} in ({self.id}, {succ.id}] -> return succ {succ.address}")
            return succ
        
        # Otherwise, ask the closest preceding node
        n0 = self.closest_preceding_node(key)
        # logger.debug(f"find_successor: closest_preceding_node -> {n0.id}@{n0.address}")
        if n0.address == self.address:
            # We are the closest, return our successor
            return succ
        
        # Remote call to find successor
        try:
            channel = grpc.insecure_channel(n0.address)
            stub = ChordServiceStub(channel)
            result = stub.FindSuccessor(ID(id=key), timeout=TIMEOUT)
            channel.close()
            logger.debug(f"find_successor: remote returned {result.id}@{result.address}")
            return result
        except Exception as e:
            logger.error(f"Remote find_successor failed: {e}")
            return succ


    def closest_preceding_node(self, key: int) -> NodeInfo:
        """Find the closest finger preceding key"""
        with self.lock:
            
            for i in range(M_BITS - 1, -1, -1):
                if self.finger[i] and is_in_interval(self.finger[i].id, self.id, key):
                    return self.finger[i]

            return NodeInfo(id=self.id, address=self.address)
        

    def get_time(self) -> float:
        """  Get the current time from the Chord network """

        # Get current leader node
        try:
            n0 = self.find_successor(0)
        except Exception as e:
            logger.error(f"Failed to get leader node from Chord network: {e}")
            return time.time()

        if not n0.address:
            logger.error("Failed to get time from Chord network")
            return time.time()
        
        # Get time from leader node
        try:
            channel = grpc.insecure_channel(n0.address)
            stub = ChordServiceStub(channel)
            response = stub.GetTime(Empty(), timeout=TIMEOUT)
            channel.close()
        except Exception as e:
            logger.error(f"Failed to get time from leader node: {e}")
            return time.time()

        return float(response.timestamp)

    def get_datetime(self) -> str:
        return datetime.fromtimestamp(self.get_time(), timezone.utc).isoformat()    
    
    def now_version(self) -> int:
        """ Get current version based on Chord network time """
        return int(self.get_time() * 1000)
    

    def serve(self):
        logger.info(f'Starting Chord node at {self.address} with ID {self.id}')

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_ChordServiceServicer_to_server(self, server)
        server.add_insecure_port(self.address)

        # Start maintenance threads
        Stabilizer(self, STABILIZE_INTERVAL).start()

        self.replicator = Replicator(self, REPLICATION_INTERVAL)
        self.replicator.start()

        server.start()
        logger.info('Chord gRPC server started')
        server.wait_for_termination()