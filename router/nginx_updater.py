import logging
import subprocess
import re
import time
from dns_cache import discover_clients

logger = logging.getLogger('nginx-updater')

CONF_FILE = '/etc/nginx/nginx.conf'
UPDATE_INTERVAL = 30
PORT = 8501


def generate_upstream(ips):
    """Generate nginx upstream block"""
    if not ips:
        return None
    
    servers = '\n'.join(
        f'        server {ip}:{PORT} max_fails=2 fail_timeout=60s;'
        for ip in ips
    )
    
    return f'''    upstream streamlit_clients {{
        hash $remote_addr consistent;
{servers}
        keepalive 32;
    }}'''


def update_nginx(ips):
    """Update nginx configuration"""
    try:
        with open(CONF_FILE, 'r') as f:
            content = f.read()
        
        # Search for current upstream block
        pattern = r'    upstream streamlit_clients \{[^}]+\}'
        current = re.search(pattern, content, re.DOTALL)
        
        new_upstream = generate_upstream(ips)
        if not new_upstream:
            logger.warning('No clients available to update')
            return False
        
        # If no changes, don't reload
        if current and current.group() == new_upstream:
            logger.debug(f'Upstream unchanged: {ips}')
            return False
        
        # Update config
        new_content = re.sub(pattern, new_upstream, content, flags=re.DOTALL)
        
        with open(CONF_FILE, 'w') as f:
            f.write(new_content)
        
        logger.info(f'Upstream updated: {ips}')
        
        # Reload nginx
        subprocess.run(['nginx', '-s', 'reload'], check=True)
        logger.info('Nginx reloaded')
        return True
        
    except Exception as e:
        logger.error(f'Error updating nginx: {e}')
        return False


def monitor():
    """Main monitoring loop"""
    logger.info('Starting client monitoring...')
    previous_ips = []
    
    while True:
        try:
            ips = discover_clients()
            
            if ips != previous_ips:
                logger.info(f'Clients changed: {previous_ips} -> {ips}')
                update_nginx(ips)
                previous_ips = ips
            
        except Exception as e:
            logger.error(f'Monitoring error: {e}')
        
        time.sleep(UPDATE_INTERVAL)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    monitor()
