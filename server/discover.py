import logging
import os
import socket

import grpc

from server.server.chord.node import ChordNode
from server.server.chord.utils.config import TIMEOUT
from server.server.config import DEFAULT_PORT
from server.server.chord.protos.chord_pb2_grpc import ChordServiceStub
from server.server.chord.protos.chord_pb2 import Empty, ID

logger = logging.getLogger('socialnet.server.discover')

def discover_nodes(address: str) -> list[str]:
    """Discover existing Chord nodes using Docker DNS"""
    existing_nodes = []
    
    try:
        network_alias = os.getenv('NETWORK_ALIAS', 'socialnet_server')
        logger.info(f'Discovering nodes via DNS lookup: {network_alias}')
        
        _, _, ip_list = socket.gethostbyname_ex(network_alias)
        
        for ip in ip_list:
            candidate_addr = f'{ip}:{DEFAULT_PORT}'
            if candidate_addr != address:
                existing_nodes.append(candidate_addr)
        
        logger.info(f'Discovered {len(existing_nodes)} potential nodes: {existing_nodes}')
    except Exception as e:
        logger.warning(f'DNS discovery failed: {e}')
    
    return existing_nodes

class NodeProxy:
    """Proxy to communicate with a remote Chord node"""
    def __init__(self, addr):
        self.address = addr
        
    def find_successor(self, id_):
        channel = grpc.insecure_channel(self.address)
        stub = ChordServiceStub(channel)
        try:
            result = stub.FindSuccessor(ID(id=id_), timeout=TIMEOUT)
            return result
        finally:
            channel.close()


def join_ring(node: ChordNode, candidate_nodes: list[str]) -> bool:
    """Try to join the Chord ring through any available node"""

    for candidate_addr in candidate_nodes:
        try:
            logger.info(f'Attempting to join ring via {candidate_addr}...')
            
            # Test if node is reachable
            channel = grpc.insecure_channel(candidate_addr)
            stub = ChordServiceStub(channel)
            stub.Ping(Empty(), timeout=TIMEOUT)
            channel.close()
            
            # Join through this node using a proxy
            proxy = NodeProxy(candidate_addr)
            node.join(proxy)
            logger.info(f'✅ Successfully joined Chord ring via {candidate_addr}')
            return True
            
        except Exception as e:
            logger.warning(f'Failed to join via {candidate_addr}: {e}')
            continue
    
    return False