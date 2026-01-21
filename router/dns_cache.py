import subprocess
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger('dns-cache')

CACHE_FILE = '/tmp/dns_cache.json'
CACHE_TTL = 300  # 5 minutes


def load_cache() -> Dict:
    """Load DNS cache from file"""
    try:
        with open(CACHE_FILE, 'r') as f:
            data = json.load(f)
            # Check if cache is still valid
            if datetime.fromisoformat(data['timestamp']) > datetime.now() - timedelta(seconds=CACHE_TTL):
                logger.debug(f'Cache valid: {data["ips"]}')
                return data
            else:
                logger.debug('Cache expired')
    except:
        pass
    return {'ips': [], 'timestamp': datetime.now().isoformat()}


def save_cache(ips: List[str]):
    """Save DNS cache to file"""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump({
                'ips': ips,
                'timestamp': datetime.now().isoformat()
            }, f)
        logger.debug(f'Cache saved: {ips}')
    except Exception as e:
        logger.error(f'Error saving cache: {e}')


def discover_clients() -> List[str]:
    """Discover clients via DNS, with fallback to cache"""
    try:
        result = subprocess.run(
            ['nslookup', 'socialnet_client'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        ips = []
        for line in result.stdout.split('\n'):
            if 'Address:' in line:
                ip = line.split('Address:')[-1].strip()
                if ip and not ip.startswith('127.0.0.1'):
                    ips.append(ip)
        
        ips = sorted(list(set(ips)))
        
        if ips:
            logger.info(f'DNS resolved: {ips}')
            save_cache(ips)
            return ips
        else:
            logger.warning('DNS returned no IPs, using cache')
            cached = load_cache()
            return cached['ips']
            
    except Exception as e:
        logger.error(f'DNS failed: {e}, using cache')
        cached = load_cache()
        return cached['ips']
