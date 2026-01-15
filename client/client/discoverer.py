import logging
import socket
import threading
import time
import os
import subprocess
import grpc
import streamlit as st
from typing import Optional

from client.client.constants import *

logger = logging.getLogger('socialnet.client.discoverer')
logger.setLevel(logging.INFO)

GLOBAL_SERVER = None
GLOBAL_BACKGROUND_CHECK_STARTED = False


class NoServersAvailableError(ConnectionError):
    """Raised when the client cannot find any alive server to contact."""


NO_SERVERS_AVAILABLE_MESSAGE = 'No servers are available right now. Please try again later.'


def _set_global_server(new_server: Optional[str], *, source: str) -> None:
    """Update GLOBAL_SERVER and log only when it changes."""
    global GLOBAL_SERVER
    if GLOBAL_SERVER == new_server:
        return

    previous = GLOBAL_SERVER
    GLOBAL_SERVER = new_server

    if new_server:
        if previous:
            logger.info('Active server changed (%s): %s -> %s', source, previous, new_server)
        else:
            logger.info('Active server selected (%s): %s', source, new_server)
    else:
        if previous:
            logger.warning('No active servers available (%s). Previous was %s', source, previous)
        else:
            logger.warning('No active servers available (%s).', source)

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
    port = int(service)
    server = st.session_state.get('server', None)

    if server and is_alive(server, port):
        return f'{server}:{port}'

    previous = server
    update_server()
    server = st.session_state.get('server')

    if server and is_alive(server, port):
        if server != previous:
            logger.info(f"Using server {server} for port {port}")
        return f'{server}:{port}'

    raise NoServersAvailableError(NO_SERVERS_AVAILABLE_MESSAGE)

def update_server():
    try:
        server_info = discover()
    except Exception as e:
        logger.info(f'Server discovery failed: {e}')
        server_info = None

    if server_info:
        host_to_use = server_info[1]
        st.session_state['server'] = host_to_use
        _set_global_server(host_to_use, source='foreground')
    else:
        if st.session_state.get('server'):
            del st.session_state['server']
        _set_global_server(None, source='foreground')

def is_alive(host, port, timeout=10):
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            logger.debug(f'{host}:{port} is reachable')
            return True
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        logger.debug(f'{host}:{port} not reachable: {e}')
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
            logger.warning(f'{env_host}:{env_port} not alive, falling back to DNS discovery')

    # resolve service name via nslookup
    service_name = os.getenv('SERVICE_SERVER', 'socialnet_server')
    try:
        result = subprocess.run(['nslookup', service_name], capture_output=True, text=True, check=True)
        logger.debug(f'nslookup output:\n{result.stdout}')

        ips = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith('Address:') and not line.startswith('Address: 127.0.0.1'):
                ip = line.split('Address:')[-1].strip()
                ips.append(ip)

        for ip in ips:
            port_to_check = int(env_port) if env_port else int(AUTH)
            if is_alive(ip, port_to_check):
                logger.info(f'Discovered alive server via DNS: {ip}')
                return ip, ip

        logger.warning('No alive servers found via nslookup')

    except subprocess.CalledProcessError as e:
        stderr = getattr(e, 'stderr', '')
        logger.error(f'nslookup failed: {e} {stderr}')

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
    try:
        server_info = discover()
    except Exception:
        server_info = None

    if server_info:
        _set_global_server(server_info[1], source='background')
    else:
        _set_global_server(None, source='background')

def periodic_server_check():
    while True:
        try:
            auth_port = int(AUTH) if isinstance(AUTH, str) and AUTH.isdigit() else AUTH
            current = GLOBAL_SERVER
            if not current or not is_alive(current, int(auth_port)):
                logger.info('Refreshing active server (background)')
                update_server_background()
        except Exception as e:
            logger.warning(f'Background server check failed: {e}')

        time.sleep(SERVER_CHECK_INTERVAL)

def start_background_check():
    global GLOBAL_BACKGROUND_CHECK_STARTED
    if not GLOBAL_BACKGROUND_CHECK_STARTED:
        threading.Thread(target=periodic_server_check, daemon=True).start()
        GLOBAL_BACKGROUND_CHECK_STARTED = True
        logger.info('Background server discovery started')
