import os
import logging
import threading
import grpc

logger = logging.getLogger('socialnet.tls_config')


class TLSConfig:
    def __init__(self, 
                 ca_cert_path='certs/ca.crt',
                 client_cert_path='certs/client.crt',
                 client_key_path='certs/client.key'):
        self.ca_cert_path = ca_cert_path
        self.client_cert_path = client_cert_path
        self.client_key_path = client_key_path
        self._verify_certificates()
    
    def _verify_certificates(self):
        """Check if the certificate files exist."""
        paths = {
            'CA': self.ca_cert_path,
            'Client Cert': self.client_cert_path,
            'Client Key': self.client_key_path,
        }
        for name, path in paths.items():
            if not os.path.exists(path):
                logger.warning(f'{name} not found at {path}')
    
    
    def load_credentials(self) -> grpc.ChannelCredentials:
        """Load client credentials for mTLS."""
        try:
            with open(self.ca_cert_path, 'rb') as f:
                root_certs = f.read()
            with open(self.client_cert_path, 'rb') as f:
                client_cert = f.read()
            with open(self.client_key_path, 'rb') as f:
                client_key = f.read()
            
            credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certs,
                private_key=client_key,
                certificate_chain=client_cert
            )
            logger.info('Client credentials with mTLS')
            return credentials
        except Exception as e:
            logger.error(f'Error in client credentials: {e}')
            return None


_config = None
lock = threading.Lock()

def get_tls_config():
    global _config
    with lock:
        if _config is None:
            _config = TLSConfig(os.getenv("CA_CERT_PATH"), os.getenv("SSL_CERT_PATH") , os.getenv("SSL_KEY_PATH"))
        return _config


def secure_channel(host: str, options=None) -> grpc.Channel:
    """Create and return a gRPC channel to the specified host."""
    if options is None:
        options = []

    credentials = get_tls_config().load_credentials()
    if credentials:
        channel = grpc.secure_channel(host, credentials, options=options)
        return channel
    
    channel = grpc.insecure_channel(host, options=options)
    return channel