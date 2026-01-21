class NoServersAvailableError(ConnectionError):
    """Raised when the client cannot find any alive server to contact."""


NO_SERVERS_AVAILABLE_MESSAGE = 'No servers are available right now. Please try again later.'

# Cache configuration
SERVER_CACHE_KEY = 'discovered_servers'
MAX_CACHE_AGE = 3600  # 1 hour in seconds

# Service ports
AUTH = 50000
POST = 50001
RELATIONS = 50002

SERVER_CHECK_INTERVAL = 20

MIN_USERNAME_LENGTH = 3
MAX_USERNAME_LENGTH = 10
MAX_POST_LENGHT = 65535

MAX_RETRIES = 3
RETRY_DELAY = 3.0