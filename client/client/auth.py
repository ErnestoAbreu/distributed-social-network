import logging
import grpc

from protos.auth_pb2 import RegisterRequest, LoginRequest
from protos.auth_pb2_grpc import AuthServiceStub
from protos import models_pb2

from client.client.discoverer import get_host
from client.client.constants import *

logger = logging.getLogger('socialnet.client.auth')
# logger.setLevel(logging.INFO)

def register(username, email, name, password):
    host = get_host(AUTH)
    channel = grpc.insecure_channel(host)
    stub = AuthServiceStub(channel)
    user = models_pb2.User(user_id=username, email=email, name=name, password_hash=password)
    request = RegisterRequest(user=user)

    try:
        response = stub.Register(request)
        return response
    except grpc.RpcError as e:
        logger.error(f'An error occurred creating the user: {e.code()}: {e.details()}')
        return False
    
def login(username, password):
    host = get_host(AUTH)
    channel = grpc.insecure_channel(host)
    stub = AuthServiceStub(channel)
    request = LoginRequest(username=username, password=password)

    try:
        response = stub.Login(request)
        return response.token
    except grpc.RpcError as e:
        logger.error(f'An error occurred logging in {e.code()}: {e.details()}')
        return None