import os
import logging
import threading
import grpc
from server.server.config import USE_TLS

logger = logging.getLogger('socialnet.tls_config')


class TLSConfig:
    def __init__(self, 
                 ca_cert_path='certs/ca.crt',
                 server_cert_path='certs/server.crt',
                 server_key_path='certs/server.key'):
        self.ca_cert_path = ca_cert_path
        self.server_cert_path = server_cert_path
        self.server_key_path = server_key_path
        self._verify_certificates()

        self.lock = threading.Lock()
    
    def _verify_certificates(self):
        """Check if the certificate files exist."""
        paths = {
            'CA': self.ca_cert_path,
            'Server Cert': self.server_cert_path,
            'Server Key': self.server_key_path,
        }
        for name, path in paths.items():
            if not os.path.exists(path):
                logger.warning(f'{name} not found at {path}')
    
    
    def load_credentials(self) -> grpc.ChannelCredentials:
        """Load server credentials for mTLS."""
        try:
            with open(self.ca_cert_path, 'rb') as f:
                root_certs = f.read()
            with open(self.server_cert_path, 'rb') as f:
                server_cert = f.read()
            with open(self.server_key_path, 'rb') as f:
                server_key = f.read()
            
            credentials = grpc.ssl_server_credentials(
                [(server_key, server_cert)],
                root_certificates=root_certs,
                require_client_auth=True
            )
            logger.info('Server credentials with mTLS')
            return credentials
        except Exception as e:
            logger.error(f'Error in server credentials: {e}')
            return None


_config = None
lock = threading.Lock()

def get_tls_config():
    global _config
    with lock:
        if _config is None:
            _config = TLSConfig(os.getenv("CA_CERT_PATH"), os.getenv("SSL_CERT_PATH") , os.getenv("SSL_KEY_PATH"))
        return _config


def create_channel(host: str, options=None) -> grpc.Channel:
    """Create and return a gRPC channel to the specified host.
    
    This function respects the USE_TLS configuration and creates either a secure
    or insecure channel accordingly.
    
    Args:
        host: The target host address in format 'host:port'
        options: Optional list of gRPC channel options
        
    Returns:
        A gRPC channel (secure or insecure depending on configuration)
    """
    if options is None:
        options = []

    if not USE_TLS:
        return grpc.insecure_channel(host, options=options)

    credentials = get_tls_config().load_credentials()
    if credentials:
        return grpc.secure_channel(host, credentials, options=options)
    
    logger.warning(f'TLS enabled but credentials failed to load for {host}, using insecure channel')
    return grpc.insecure_channel(host, options=options)