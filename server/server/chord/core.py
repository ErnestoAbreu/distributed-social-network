import grpc
import logging

from server.server.chord.node import ChordNode
from server.server.security import create_channel
from .utils.hashing import hash_key
from .utils.config import M_BITS, TIMEOUT, TIMEOUT_LOAD, TIMEOUT_SAVE, TIMEOUT_EXISTS, TIMEOUT_DELETE
from .protos.chord_pb2 import Key, KeyValue, NodeInfo
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

        if responsible_node is None:
            responsible_node = NodeInfo(id=node.id, address=node.address)
        
        if responsible_node.address == node.address:
            return node.storage.exists(key), None

        channel = create_channel(responsible_node.address)
        stub = ChordServiceStub(channel)
        
        response = stub.Get(Key(key=key), timeout=TIMEOUT_EXISTS)
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

        if responsible_node is None:
            responsible_node = NodeInfo(id=node.id, address=node.address)
        
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
        
        channel = create_channel(responsible_node.address)
        stub = ChordServiceStub(channel)
        
        try:
            response = stub.Get(Key(key=key), timeout=TIMEOUT_LOAD)
            channel.close()
            
            if not response.value:
                return None, grpc.StatusCode.NOT_FOUND
            
            try:
                prototype.ParseFromString(response.value.encode('latin1'))
                return prototype, None
            except Exception as e:
                logger.error(f"Failed to parse protobuf for key {key}: {e}")
                return None, grpc.StatusCode.INTERNAL
            
        except grpc.RpcError as e:
            channel.close()
            logger.warning(f"RPC error loading key {key} from {responsible_node.address} (timeout={TIMEOUT_LOAD}s): {e}. Attempting local fallback...")
            
            # Fallback: try to load from local storage if the remote node is unreachable
            value = node.storage.get(key)
            if value:
                try:
                    prototype.ParseFromString(value.encode('latin1'))
                    logger.info(f"Successfully loaded key {key} from local storage (fallback)")
                    return prototype, None
                except Exception as parse_err:
                    logger.error(f"Failed to parse protobuf for key {key}: {parse_err}")
                    return None, grpc.StatusCode.INTERNAL
            
            logger.error(f"RPC error loading key {key} and no local fallback available")
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

        if responsible_node is None:
            responsible_node = NodeInfo(id=node.id, address=node.address)
        
        serialized_value = prototype.SerializeToString().decode('latin1')
        
        if responsible_node.address == node.address:
            node.storage.put(key, serialized_value, node.now_version())
            return None
        
        channel = create_channel(responsible_node.address)
        stub = ChordServiceStub(channel)
        
        try:
            stub.Put(KeyValue(key=key, value=serialized_value), timeout=TIMEOUT_SAVE)
            channel.close()
            return None
            
        except grpc.RpcError as e:
            channel.close()
            logger.warning(f"RPC error saving key {key} to {responsible_node.address} (timeout={TIMEOUT_SAVE}s): {e}. Saving to local storage as fallback...")
            
            # Fallback: save to local storage if the remote node is unreachable
            # This ensures data is not lost and can be replicated later
            node.storage.put(key, serialized_value, node.now_version())
            logger.info(f"Successfully saved key {key} to local storage (fallback)")
            return None
            
    except Exception as e:
        logger.error(f"Error saving key {key}: {e}")
        return grpc.StatusCode.INTERNAL

def delete(node: ChordNode, key: str) -> grpc.StatusCode | None:
    """
    Delete a key from the DHT
    
    Args:
        node: ChordNode instance
        key: The key to delete
    Returns:
        grpc.StatusCode error code or None if successful
    """

    try:
        key_hash = hash_key(key, M_BITS)
        responsible_node = node.find_successor(key_hash)

        if responsible_node is None:
            responsible_node = NodeInfo(id=node.id, address=node.address)
        
        if responsible_node.address == node.address:
            node.storage.delete(key, node.now_version())
            return None
        
        channel = create_channel(responsible_node.address)
        stub = ChordServiceStub(channel)
        
        try:
            stub.Delete(Key(key=key), timeout=TIMEOUT_DELETE)
            channel.close()
            return None
            
        except grpc.RpcError as e:
            channel.close()
            logger.warning(f"RPC error deleting key {key} from {responsible_node.address} (timeout={TIMEOUT_DELETE}s): {e}. Attempting local delete as fallback...")
            
            # Fallback: delete from local storage if the remote node is unreachable
            node.storage.delete(key, node.now_version())
            logger.info(f"Successfully deleted key {key} from local storage (fallback)")
            return None
            
    except Exception as e:
        logger.error(f"Error deleting key {key}: {e}")
        return grpc.StatusCode.INTERNAL