import logging
import json
import time
from pathlib import Path

logger = logging.getLogger('socialnet.server.chord.utils.cache')

# Cache configuration
CACHE_DIR = Path('cache')
CACHE_DIR.mkdir(exist_ok=True)
NODE_CACHE_FILE = CACHE_DIR / 'discovered_nodes.json'
MAX_CACHE_AGE = 3600  # 1 hour in seconds


def load_node_cache():
    """ Load cached node addresses from disk. """
    try:
        if NODE_CACHE_FILE.exists():
            with open(NODE_CACHE_FILE, 'r') as f:
                cache_data = json.load(f)
                nodes = cache_data.get('nodes', [])
                timestamp = cache_data.get('timestamp', 0)
                
                # Check if cache is not too old
                if time.time() - timestamp < MAX_CACHE_AGE:
                    logger.debug(f'Loaded {len(nodes)} nodes from cache')
                    return nodes
                else:
                    logger.debug('Node cache expired, ignoring')
    except Exception as e:
        logger.warning(f'Failed to load node cache: {e}')
    return []


def save_node_cache(nodes):
    """ Save node addresses to disk cache. """
    try:
        cache_data = {
            'nodes': nodes,
            'timestamp': time.time()
        }
        with open(NODE_CACHE_FILE, 'w') as f:
            json.dump(cache_data, f)
        logger.debug(f'Saved {len(nodes)} nodes to cache')
    except Exception as e:
        logger.warning(f'Failed to save node cache: {e}')


def add_to_node_cache(node_addr):
    """ Add a node to the cache if not already present. """
    try:
        cached_nodes = load_node_cache()
        if node_addr not in cached_nodes:
            cached_nodes.insert(0, node_addr) 
            # Keep only last 10 nodes
            cached_nodes = cached_nodes[:10]
            save_node_cache(cached_nodes)
    except Exception as e:
        logger.warning(f'Failed to update node cache: {e}')


def clear_node_cache():
    """Clear the node cache file."""
    try:
        if NODE_CACHE_FILE.exists():
            NODE_CACHE_FILE.unlink()
            logger.debug('Cleared node cache')
    except Exception as e:
        logger.warning(f'Failed to clear node cache: {e}')
