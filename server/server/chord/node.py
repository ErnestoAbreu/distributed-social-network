import logging
import grpc
import threading
from concurrent import futures

from server.server.chord.threads.stabilize import Stabilizer
from server.server.chord.utils.utils import is_in_interval

from .protos.chord_pb2 import ID, NodeInfo, Empty, Value
from .protos.chord_pb2_grpc import ChordServiceServicer, ChordServiceStub, add_ChordServiceServicer_to_server

from .utils.hashing import hash_key
from .utils.config import *
from .storage import Storage

logger = logging.getLogger('socialnet.server.chord.node')

class ChordNode(ChordServiceServicer):
    def __init__(self, address):
        self.address = address
        self.id = hash_key(address, M_BITS)
        self.predecessor = None
        self.storage = Storage()
        self.lock = threading.Lock()

        self.finger = [None] * M_BITS
        self.next_finger = 0


    # ---------------- Chord RPCs ----------------
    def GetSuccessor(self, request, context) -> NodeInfo:
        try:
            return self.find_successor(request.id)
        except Exception as e:
            logger.error(f"finding successor failed: {e}")
            return NodeInfo(id=self.id, address=self.address)


    def GetPredecessor(self, request, context) -> NodeInfo:
        if self.predecessor:
            return NodeInfo(id=self.predecessor.id, address=self.predecessor.address)
        return NodeInfo(id=self.id, address=self.address)


    def UpdatePredecessor(self, request, context) -> Empty:
        try:
            with self.lock:
                # Update predecessor if we don't have one, or if the request
                # is between our current predecessor and ourselves.
                if (not self.predecessor) or is_in_interval(request.id, self.predecessor.id, self.id):
                    self.predecessor = request
                    logger.debug(f"UpdatePredecessor: predecessor set to {request.id}@{request.address}")
        except Exception as e:
            logger.error(f"UpdatePredecessor error: {e}")
        return Empty()


    def Ping(self, request, context):
        return Empty()


    def Get(self, request, context):
        val = self.storage.get(request.key)
        return Value(value=val if val else "")


    def Put(self, request, context):
        self.storage.put(request.key, request.value)
        return Empty()


    def Delete(self, request, context):
        self.storage.delete(request.key)
        return Empty()

    # ---------------- Chord Logic ----------------
    def join(self, known_node):
        if known_node:
            logger.info(f"Joining Chord ring via node {known_node.address}")

            channel = grpc.insecure_channel(known_node.address)
            stub = ChordServiceStub(channel)
            successor = stub.GetSuccessor(ID(id=self.id), timeout=TIMEOUT)
            channel.close()

            if successor and successor.address:
                self.finger[0] = successor
            else:
                logger.error('find_successor returned invalid result')
                logger.info("Creating new Chord ring")
                self.finger[0] = NodeInfo(id=self.id, address=self.address)
        else:
            logger.info("Creating new Chord ring")
            self.finger[0] = NodeInfo(id=self.id, address=self.address)
        

    def find_successor(self, key) -> NodeInfo:
        # read successor atomically
        with self.lock:
            succ = self.finger[0] or NodeInfo(id=self.id, address=self.address)

        logger.debug(f"find_successor: key={key} my_id={self.id} succ={getattr(succ,'id',None)}@{getattr(succ,'address',None)}")

        # If we are the only node (successor is self), return ourselves
        if succ.address == self.address:
            logger.debug("find_successor: single node ring -> return self")
            return succ
        
        # If id is between us and our successor (handles wrap-around)
        if is_in_interval(key, self.id, succ.id, inclusive_end=True):
            logger.debug(f"find_successor: key in ({self.id}, {succ.id}] -> return succ {succ.address}")
            return succ
        
        # Otherwise, ask the closest preceding node
        n0 = self.closest_preceding_node(key)
        logger.debug(f"find_successor: closest_preceding_node -> {n0.id}@{n0.address}")
        if n0.address == self.address:
            # We are the closest, return our successor
            return succ
        
        # Remote call to find successor
        try:
            channel = grpc.insecure_channel(n0.address)
            stub = ChordServiceStub(channel)
            result = stub.GetSuccessor(ID(id=key), timeout=TIMEOUT)
            channel.close()
            logger.debug(f"find_successor: remote returned {result.id}@{result.address}")
            return result
        except Exception as e:
            logger.warning(f"Remote find_successor failed: {e}")
            return succ

    def closest_preceding_node(self, key) -> NodeInfo:
        """Find the closest finger preceding key"""
        with self.lock:
            
            for i in range(M_BITS - 1, -1, -1):
                if self.finger[i] and is_in_interval(self.finger[i].id, self.id, key):
                    return self.finger[i]

            return NodeInfo(id=self.id, address=self.address)


    def ping_node(self, node):
        """Ping a node to check if it's alive"""
        if not node or node.address == self.address:
            return True
        try:
            channel = grpc.insecure_channel(node.address)
            stub = ChordServiceStub(channel)
            stub.Ping(Empty(), timeout=TIMEOUT)
            channel.close()
            return True
        except Exception:
            return False


    def serve(self):
        logger.info(f'Starting Chord node at {self.address} with ID {self.id}')

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_ChordServiceServicer_to_server(self, server)
        server.add_insecure_port(self.address)

        # Start maintenance threads
        Stabilizer(self, STABILIZE_INTERVAL).start()

        server.start()
        logger.info('Chord gRPC server started')
        server.wait_for_termination()