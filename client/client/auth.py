import logging
import grpc

from protos.auth_pb2 import RegisterRequest, LoginRequest
from protos.auth_pb2_grpc import AuthServiceStub
from protos import models_pb2

from client.client.discoverer import get_host
from client.client.config import *
from client.client.utils import retry_on_failure
from client.client.security import create_channel

logger = logging.getLogger('socialnet.client.auth')
# logger.setLevel(logging.INFO)

@retry_on_failure()
def register(username, email, name, password):
    host = get_host(AUTH)
    channel = create_channel(host)
    stub = AuthServiceStub(channel)
    user = models_pb2.User(user_id=username, email=email, name=name, password_hash=password)
    request = RegisterRequest(user=user)

    response = stub.Register(request)
    return response


@retry_on_failure()
def login(username, password):
    host = get_host(AUTH)
    channel = create_channel(host)
    stub = AuthServiceStub(channel)
    request = LoginRequest(username=username, password=password)
    
    response = stub.Login(request)
    return response.token
    