import grpc
import logging

from server.server.chord.node import ChordNode
from .utils.hashing import hash_key
from .utils.config import M_BITS, TIMEOUT
from .protos.chord_pb2 import Key, KeyValue
from .protos.chord_pb2_grpc import ChordServiceStub

logger = logging.getLogger('socialnet.chord.core')


def exists(node: ChordNode, key: str) -> tuple[bool, grpc.StatusCode | None]:
    """
    Check if a key exists in the DHT
    
    Args:
        node: ChordNode instance
        key: The key to check
        
    Returns:
        True if key exists, False otherwise
    """
    try:
        key_hash = hash_key(key, M_BITS)
        responsible_node = node.find_successor(key_hash)
        
        if responsible_node.address == node.address:
            return node.storage.exists(key), None

        channel = grpc.insecure_channel(responsible_node.address)
        stub = ChordServiceStub(channel)
        
        response = stub.Get(Key(key=key), timeout=TIMEOUT)
        exists_value = response.value != ""
        
        channel.close()
        return exists_value, None
    except Exception as e:
        logger.error(f"Error checking existence of key {key}: {e}")
        return False, grpc.StatusCode.INTERNAL


def load(node: ChordNode, key: str, prototype) -> tuple[object, grpc.StatusCode | None]:
    """
    Load a protobuf message
    
    Args:
        node: ChordNode instance
        key: The key to load
        prototype: Empty protobuf message instance to deserialize into
        
    Returns:
        Tuple of (message, error_code)
        - message: The loaded protobuf message or None
        - error_code: grpc.StatusCode or None if successful
    """
    try:
        key_hash = hash_key(key, M_BITS)
        responsible_node = node.find_successor(key_hash)
        
        if responsible_node.address == node.address:
            value = node.storage.get(key)
            if not value:
                return None, grpc.StatusCode.NOT_FOUND
            
            try:
                prototype.ParseFromString(value.encode('latin1'))
                return prototype, None
            except Exception as e:
                logger.error(f"Failed to parse protobuf for key {key}: {e}")
                return None, grpc.StatusCode.INTERNAL
        
        channel = grpc.insecure_channel(responsible_node.address)
        stub = ChordServiceStub(channel)
        
        try:
            response = stub.Get(Key(key=key), timeout=TIMEOUT)
            channel.close()
            
            if not response.value:
                return None, grpc.StatusCode.NOT_FOUND
            
            try:
                prototype.ParseFromString(value.encode('latin1'))
                return prototype, None
            except Exception as e:
                logger.error(f"Failed to parse protobuf for key {key}: {e}")
                return None, grpc.StatusCode.INTERNAL
            
        except grpc.RpcError as e:
            channel.close()
            logger.error(f"RPC error loading key {key}: {e}")
            return None, grpc.StatusCode.INTERNAL
            
    except Exception as e:
        logger.error(f"Error loading key {key}: {e}")
        return None, grpc.StatusCode.INTERNAL


def save(node: ChordNode, key: str, prototype: object) -> grpc.StatusCode | None:
    """
    Save a protobuf message
    
    Args:
        node: ChordNode instance
        key: The key to save under
        prototype: Protobuf message to save
        
    Returns:
        grpc.StatusCode error code or None if successful
    """
    try:
        key_hash = hash_key(key, M_BITS)
        responsible_node = node.find_successor(key_hash)
        
        serialized_value = prototype.SerializeToString().decode('latin1')
        
        if responsible_node.address == node.address:
            node.storage.put(key, serialized_value)
            return None
        
        channel = grpc.insecure_channel(responsible_node.address)
        stub = ChordServiceStub(channel)
        
        try:
            stub.Put(KeyValue(key=key, value=serialized_value), timeout=TIMEOUT)
            channel.close()
            return None
            
        except grpc.RpcError as e:
            channel.close()
            logger.error(f"RPC error saving key {key}: {e}")
            return grpc.StatusCode.INTERNAL
            
    except Exception as e:
        logger.error(f"Error saving key {key}: {e}")
        return grpc.StatusCode.INTERNAL
