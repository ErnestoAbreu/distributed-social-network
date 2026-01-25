import os
import socket
import ipaddress
import logging
import threading
import grpc
from datetime import datetime, timedelta
from cryptography import x509
from cryptography.x509.oid import NameOID, ExtensionOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from client.client.config import USE_TLS

logger = logging.getLogger('socialnet.tls_config')


class TLSConfig:
    def __init__(self, ca_cert_path='certs/ca.crt', ca_key_path='certs/ca.key'):
        self.ca_cert_path = ca_cert_path
        self.ca_key_path = ca_key_path
        self._generate_certificate()
    
    def _generate_certificate(self):
        """Generate client certificate if CA is available."""
        try:
            if os.path.exists(self.ca_cert_path) and os.path.exists(self.ca_key_path):
                cert_pem, key_pem = generate_certificate(
                    self.ca_cert_path, 
                    self.ca_key_path
                )
                # Store in memory
                self._cert = cert_pem
                self._key = key_pem
            else:
                logger.warning(f'CA not found at {self.ca_cert_path}, cannot generate certificate')
                self._cert = None
                self._key = None
        except Exception as e:
            logger.error(f'Failed to generate certificate: {e}')
            self._cert = None
            self._key = None
    
    
    def load_credentials(self) -> grpc.ChannelCredentials:
        """Load client credentials for mTLS."""
        try:
            # Load CA certificate
            with open(self.ca_cert_path, 'rb') as f:
                root_certs = f.read()
            
            credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certs,
                private_key=self._key,
                certificate_chain=self._cert
            )
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
            ca_cert_path = os.getenv("CA_CERT_PATH", "certs/ca.crt")
            ca_key_path = os.getenv("CA_KEY_PATH", "certs/ca.key")
            _config = TLSConfig(ca_cert_path, ca_key_path)
        return _config


def create_channel(host: str, options=None) -> grpc.Channel:
    """Create and return a gRPC channel to the specified host."""
    if options is None:
        options = []

    if not USE_TLS:
        return grpc.insecure_channel(host, options=options)

    credentials = get_tls_config().load_credentials()
    if credentials:
        return grpc.secure_channel(host, credentials, options=options)

    logger.warning(f'TLS enabled but credentials failed to load, using insecure channel to {host}')
    return grpc.insecure_channel(host, options=options)


def generate_certificate(ca_cert_path, ca_key_path, client_ip=None):
    try:
        # Auto-detect IP if not provided
        if client_ip is None:
            client_ip = socket.gethostbyname(socket.gethostname())
        
        logger.info(f'Generating client certificate for IP: {client_ip}')
        
        # Load CA certificate and key
        with open(ca_cert_path, 'rb') as f:
            ca_cert_data = f.read()
            ca_cert = x509.load_pem_x509_certificate(ca_cert_data, default_backend())
        
        with open(ca_key_path, 'rb') as f:
            ca_key_data = f.read()
            ca_key = serialization.load_pem_private_key(
                ca_key_data, 
                password=None, 
                backend=default_backend()
            )
        
        # Generate private key for client
        client_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        
        # Build subject name
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f'socialnet-client-{client_ip}'),])
        
        # Build SAN (Subject Alternative Name) with IP and DNS names
        san_list = [x509.DNSName('localhost')]

        # Add IP to SAN if valid
        try:
            ip_obj = ipaddress.ip_address(client_ip)
            san_list.append(x509.IPAddress(ip_obj))
        except ValueError:
            logger.warning(f'Invalid IP address for SAN: {client_ip}, skipping IP entry')
        
        # Create certificate
        cert_builder = x509.CertificateBuilder()
        cert_builder = cert_builder.subject_name(subject)
        cert_builder = cert_builder.issuer_name(ca_cert.subject)
        cert_builder = cert_builder.public_key(client_key.public_key())
        cert_builder = cert_builder.serial_number(x509.random_serial_number())
        cert_builder = cert_builder.not_valid_before(datetime.utcnow())
        cert_builder = cert_builder.not_valid_after(datetime.utcnow() + timedelta(days=365))
        
        # Add extensions
        cert_builder = cert_builder.add_extension(
            x509.SubjectAlternativeName(san_list),
            critical=False
        )
        cert_builder = cert_builder.add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True
        )
        cert_builder = cert_builder.add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False
            ),
            critical=True
        )
        cert_builder = cert_builder.add_extension(
            x509.ExtendedKeyUsage([
                x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                x509.oid.ExtendedKeyUsageOID.SERVER_AUTH
            ]),
            critical=False
        )
        
        # Sign certificate with CA
        cert = cert_builder.sign(ca_key, hashes.SHA256(), default_backend())
        
        # Serialize certificate and key
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = client_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        logger.info(f'Client certificate generated successfully for {client_ip}')
        return cert_pem, key_pem
        
    except Exception as e:
        logger.error(f'Error generating client certificate: {e}')
        raise