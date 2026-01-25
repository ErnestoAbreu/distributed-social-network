import os

DEFAULT_PORT = 10000

# TLS configuration
USE_TLS = os.getenv('USE_TLS', 'false').lower() in ('true', '1', 'yes')