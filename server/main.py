import logging
import threading
import time
import signal
import sys
import socket
import os
import grpc

from server.discover import discover_nodes, join_ring
from server.server.auth import start_auth_service, AuthRepository
from server.server.relations import start_relations_service, RelationsRepository
from server.server.posts import start_post_service, PostRepository
from server.server.chord.node import ChordNode
from server.server.config import DEFAULT_PORT

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('socialnet.server.main')

def run_services():
    host = os.getenv('NODE_HOST') or socket.gethostbyname(socket.gethostname())
    address = f'{host}:{DEFAULT_PORT}'
    node = ChordNode(address)

    logger.info(f'Chord Node initiated with IP: {host}')

    time.sleep(3)

    auth_repo = AuthRepository(node)
    post_repo = PostRepository(node)
    relations_repo = RelationsRepository(node)

    logger.info('Init gRPC services...')

    auth_thread = threading.Thread(target=start_auth_service, args=(
        '0.0.0.0:50000', auth_repo), daemon=True)
    auth_thread.start()

    post_thread = threading.Thread(target=start_post_service, args=(
        '0.0.0.0:50001', post_repo, auth_repo), daemon=True)
    post_thread.start()

    relations_thread = threading.Thread(target=start_relations_service, args=(
        '0.0.0.0:50002', relations_repo, auth_repo), daemon=True)
    relations_thread.start()

    logger.info('gRPC services listening in ports 50000, 50001, 50002')
    
    logger.info('Discovering existing Chord nodes...')

    existing_nodes = discover_nodes(node.address)
    
    if existing_nodes:
        if join_ring(node, existing_nodes):
            logger.info('Joined existing Chord ring')
        else:
            raise RuntimeError('Failed to join existing Chord ring')
    else:
        node.join(None)
    
    node.serve()

def exit_handler(signal, frame):
    logger.info('Closing server...')
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signalnum=signal.SIGINT, handler=exit_handler)
    signal.signal(signalnum=signal.SIGTERM, handler=exit_handler)

    run_services()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        exit_handler(None, None)
