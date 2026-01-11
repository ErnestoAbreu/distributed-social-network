import logging
import grpc
import threading
import time
from concurrent import futures

from .protos.chord_pb2 import NodeInfo, Empty, Value
from .protos.chord_pb2_grpc import ChordServiceServicer, add_ChordServiceServicer_to_server

from .utils.hashing import hash_key
from .utils.config import *
from .finger_table import FingerTable
from .storage import Storage
from .failure import FailureDetector

logger = logging.getLogger('socialnet.server.chord.node')

class ChordNode(ChordServiceServicer):
    def __init__(self, address):
        self.address = address
        self.id = hash_key(address, M_BITS)
        self.successor = None
        self.predecessor = None
        self.finger = FingerTable(self.id, M_BITS)
        self.storage = Storage()
        self.lock = threading.Lock()


    # ---------------- Chord RPCs ----------------
    def FindSuccessor(self, request, context):
        try:
            return self.find_successor(request.id)
        except Exception as e:
            logger.error(f"FindSuccessor failed: {e}")
            return NodeInfo(id=self.successor.id, address=self.successor.address)


    def GetPredecessor(self, request, context):
        if self.predecessor:
            return NodeInfo(id=self.predecessor.id, address=self.predecessor.address)
        return NodeInfo()


    def Notify(self, request, context):
        if not self.predecessor or request.id > self.predecessor.id:
            self.predecessor = request
        return Empty()

    def Ping(self, request, context):
        return Empty()

    # ---------------- DHT RPCs ----------------
    def Put(self, request, context):
        self.storage.put(request.key, request.value)
        return Empty()


    def Get(self, request, context):
        val = self.storage.get(request.key)
        return Value(value=val if val else "")


    def Delete(self, request, context):
        self.storage.delete(request.key)
        return Empty()

    # ---------------- Chord Logic ----------------
    def join(self, known_node):
        if known_node:
            successor = known_node.find_successor(self.id)
            if successor and successor.address:
                self.successor = successor
            else:
                logger.error('find_successor returned invalid result')
                self.successor = NodeInfo(id=self.id, address=self.address)
        else:
            self.successor = NodeInfo(id=self.id, address=self.address)
        

    def find_successor(self, id_) -> NodeInfo:
        # If we are the only node, return ourselves
        if self.successor.address == self.address:
            return self.successor
        
        # If id is between us and our successor
        if self.id < id_ <= self.successor.id:
            return self.successor
        
        # Otherwise, ask the closest preceding node
        n0 = self.closest_preceding_node(id_)
        if n0.address == self.address:
            # We are the closest, return our successor
            return self.successor
        
        # Remote call to find successor
        try:
            channel = grpc.insecure_channel(n0.address)
            from .protos.chord_pb2_grpc import ChordServiceStub
            from .protos.chord_pb2 import ID
            stub = ChordServiceStub(channel)
            result = stub.FindSuccessor(ID(id=id_), timeout=2)
            channel.close()
            return result
        except:
            return self.successor

    def closest_preceding_node(self, id_):
        for i in reversed(range(M_BITS)):
            f = self.finger.table[i]
            if f and f.address != self.address and self.id < f.id < id_:
                return f
        return NodeInfo(id=self.id, address=self.address)


    def stabilize(self):
        if not self.successor or self.successor.address == self.address:
            return
        
        try:
            channel = grpc.insecure_channel(self.successor.address)
            from .protos.chord_pb2_grpc import ChordServiceStub
            stub = ChordServiceStub(channel)
            
            x = stub.GetPredecessor(Empty(), timeout=2)
            if x and x.id != 0 and self.id < x.id < self.successor.id:
                self.successor = x
            
            stub.Notify(NodeInfo(id=self.id, address=self.address), timeout=2)
            channel.close()
        except Exception as e:
            logger.warning(f"Stabilize failed: {e}")


    def fix_fingers(self):
        # Only fix fingers if we have other nodes in the ring
        if self.successor.address == self.address:
            return
        
        for i in range(M_BITS):
            try:
                self.finger.table[i] = self.find_successor(self.finger.start(i))
            except Exception as e:
                logger.warning(f"Failed to fix finger {i}: {e}")


    def check_predecessor(self):
        if self.predecessor:
            if not self.ping_node(self.predecessor):
                logger.info(f"Predecessor {self.predecessor.address} failed, removing")
                self.predecessor = None

    def ping_node(self, node):
        """Ping a node to check if it's alive"""
        if not node or node.address == self.address:
            return True
        try:
            channel = grpc.insecure_channel(node.address)
            from .protos.chord_pb2_grpc import ChordServiceStub
            stub = ChordServiceStub(channel)
            stub.Ping(Empty(), timeout=2)
            channel.close()
            return True
        except:
            return False

    def serve(self):
        logger.info(f'Starting Chord node at {self.address} with ID {self.id}')

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_ChordServiceServicer_to_server(self, server)
        server.add_insecure_port(self.address)

        server.start()

        FailureDetector(self, PING_INTERVAL).start()

        while True:
            time.sleep(STABILIZE_INTERVAL)
            self.stabilize()
            self.fix_fingers()
            self.check_predecessor()