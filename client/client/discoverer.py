import logging
import socket
import threading
import time
import os
import subprocess
import grpc
import streamlit as st

from client.client.constants import *

logger = logging.getLogger('socialnet.client.discoverer')
logger.setLevel(logging.INFO)

GLOBAL_SERVER = None
GLOBAL_BACKGROUND_CHECK_STARTED = False

class _ClientCallDetails(grpc.ClientCallDetails):
    def __init__(self, method, timeout, metadata, credentials, wait_for_ready, compression):
        self.method = method
        self.timeout = timeout
        self.metadata = metadata
        self.credentials = credentials
        self.wait_for_ready = wait_for_ready
        self.compression = compression

def _augment_client_call_details(client_call_details, new_metadata):
    try:
        return client_call_details._replace(metadata=new_metadata)
    except Exception:
        return _ClientCallDetails(
            getattr(client_call_details, "method", None),
            getattr(client_call_details, "timeout", None),
            new_metadata,
            getattr(client_call_details, "credentials", None),
            getattr(client_call_details, "wait_for_ready", None),
            getattr(client_call_details, "compression", None),
        )

def get_host(service):
    if 'server' not in st.session_state:
        st.session_state['server'] = None

    server = st.session_state['server']

    if not server or not is_alive(server, int(service)):
        update_server()
        logger.info(f'New conection  to {server}:{service}')
        server = st.session_state['server']

    if server and is_alive(server, int(service)):
        return f'{server}:{service}'

    logger.info(server)

    raise ConnectionError('No available servers to connect')

def update_server():
    global GLOBAL_SERVER

    try:
        server_info = discover()
    except Exception as e:
        logger.info(f'No server found: {e}')
        server_info = None

    if server_info:
        logger.info(f'Found {server_info}')
        host_to_use = server_info[1]
        st.session_state['server'] = host_to_use
        GLOBAL_SERVER = host_to_use
    else:
        logger.info('No server found')
        if st.session_state.get('server'):
            del st.session_state['server']
        GLOBAL_SERVER = None

def is_alive(host, port, timeout=10):
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            logger.info(f'{host}:{port} handshake done')
            return True
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        logger.info(f'{host}:{port} not reachable: {e}')
        return False

def discover():
    env_host = os.getenv('SERVER_HOST')
    env_port = os.getenv('SERVER_PORT')

    # try environment variables first
    if env_host and env_port:
        logger.info(f'Using SERVER_HOST/SERVER_PORT from environment: {env_host}:{env_port}')
        if is_alive(env_host, int(env_port)):
            return env_host, env_host
        else:
            logger.warning(f'{env_host}:{env_port} not alive, trying nslookup')

    # resolve service name via nslookup
    service_name = 'socialnet_server'
    try:
        result = subprocess.run(['nslookup', service_name], capture_output=True, text=True, check=True)
        logger.info(f'nslookup result:\n{result.stdout}')

        ips = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith('Address:') and not line.startswith('Address: 127.0.0.1'):
                ip = line.split('Address:')[-1].strip()
                ips.append(ip)

        for ip in ips:
            port_to_check = int(env_port) if env_port else int(AUTH)
            if is_alive(ip, port_to_check):
                logger.info(f'Using alive server {ip}')
                return ip, ip

        logger.warning('No alive servers found via nslookup')

    except subprocess.CalledProcessError as e:
        logger.error(f'nslookup failed: {e}')

    raise RuntimeError('No server found')

class AuthInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor):
    def __init__(self, token):
        self.token = token

    def _add_auth_metadata(self, client_call_details):
        metadata = client_call_details.metadata if client_call_details.metadata else []
        metadata = list(metadata) + [('authorization', self.token)]
        return _augment_client_call_details(client_call_details, metadata)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_cd = self._add_auth_metadata(client_call_details)
        return continuation(new_cd, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_cd = self._add_auth_metadata(client_call_details)
        return continuation(new_cd, request)

def get_authenticated_channel(host, token):
    auth_interceptor = AuthInterceptor(token)
    channel = grpc.insecure_channel(host)
    return grpc.intercept_channel(channel, auth_interceptor)

def update_server_background():
    global GLOBAL_SERVER
    try:
        server_info = discover()
    except Exception:
        server_info = None

    if server_info:
        logger.info(f'Discover found {server_info} (background)')
        GLOBAL_SERVER = server_info[1]
    else:
        logger.info('No server found (background)')
        GLOBAL_SERVER = None

def periodic_server_check():
    global GLOBAL_SERVER
    while True:
        auth_port = int(AUTH) if isinstance(AUTH, str) and AUTH.isdigit() else AUTH
        if not GLOBAL_SERVER or not is_alive(GLOBAL_SERVER, int(auth_port)):
            logger.info('Current server is not alive. Updating server info (background)')
            update_server_background()
        time.sleep(SERVER_CHECK_INTERVAL)

def start_background_check():
    global GLOBAL_BACKGROUND_CHECK_STARTED
    if not GLOBAL_BACKGROUND_CHECK_STARTED:
        threading.Thread(target=periodic_server_check, daemon=True).start()
        GLOBAL_BACKGROUND_CHECK_STARTED = True
