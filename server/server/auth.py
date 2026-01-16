import logging
import os
import grpc
import jwt
import datetime
from datetime import timezone

from concurrent import futures

from protos.models_pb2 import User
from protos.auth_pb2 import RegisterResponse, LoginResponse
from protos.auth_pb2_grpc import AuthServiceServicer, add_AuthServiceServicer_to_server
from server.server.chord.core import exists, load, save

logger = logging.getLogger('socialnet.server.auth')

class AuthRepository:
    def __init__(self, node) -> None:
        self.node = node

    def create_key(self, username: str) -> str:
        return os.path.join('User', username.lower())

    def exists_user(self, username: str) -> tuple[bool, grpc.StatusCode | None]:
        logger.info(f'Check if username exists: {username}')
        key = self.create_key(username)
        return exists(self.node, key)

    def load_user(self, username: str) -> tuple[User, grpc.StatusCode | None]:
        logger.info(f'Load user: {username}')

        key = self.create_key(username)
        user, error = load(self.node, key, User())

        if error == grpc.StatusCode.NOT_FOUND:
            return None, grpc.StatusCode.NOT_FOUND

        if error:
            return None, grpc.StatusCode.INTERNAL

        return user, None

    def save_user(self, user) -> grpc.StatusCode | None:
        username = user.user_id

        logger.info(f'Save user: {username}')
        
        key = self.create_key(username)
        error = save(self.node, key, user)

        if error:
            return grpc.StatusCode.INTERNAL

        return None


class AuthService(AuthServiceServicer):
    def __init__(self, auth_repo: AuthRepository, jwt_private_key, jwt_algorithm='HS256'):
        self.auth_repo = auth_repo
        self.jwt_private_key = jwt_private_key
        self.jwt_algorithm = jwt_algorithm

    def Register(self, request, context):
        user = request.user

        logger.info(f'Register an user: {user.user_id}')

        exists, error = self.auth_repo.exists_user(user.user_id)
        if exists:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'User already exists')
            return RegisterResponse(success=False)
        if error:
            context.abort(error, 'Registration failed')
            return RegisterResponse(success=False)

        error = self.auth_repo.save_user(user)
        if error:
            logger.error('Registration failed')
            return RegisterResponse(success=False, message='Registration failed')

        logger.info('Successful registration')
        return RegisterResponse(success=True, message='User created successfully')

    def Login(self, request, context):
        username = request.username
        password = request.password

        logger.info(f'Login user: {username}')

        user, error = self.auth_repo.load_user(username)

        if error:
            if error == grpc.StatusCode.NOT_FOUND:
                context.abort(grpc.StatusCode.PERMISSION_DENIED, 'Incorrect username or password')
            else:
                context.abort(error, 'Login failed')

        if user is None:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, 'Incorrect username or password')

        if not user or user.password_hash != password:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, 'Incorrect username or password')

        logger.info('Successful login')
        token = self.gen_token(user)
        return LoginResponse(token=token)

    def gen_token(self, user):
        expiration = (datetime.datetime.now(timezone.utc) + datetime.timedelta(hours=24)).isoformat()

        payload = {
            'user_id': user.user_id,
            'expires': expiration
        }

        token = jwt.encode(payload, self.jwt_private_key, algorithm=self.jwt_algorithm)
        return token


def start_auth_service(addr, auth_repo: AuthRepository, max_workers: int = 10):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

    SECRET_KEY = os.getenv("SECRET_KEY", 'dev_secret_123')
    add_AuthServiceServicer_to_server(AuthService(auth_repo, SECRET_KEY, 'HS256'), server)

    server.add_insecure_port(addr)
    server.start()
    
    port = str(addr).split(':')
    logger.info(f'Auth service started on port {port[1]}')
    server.wait_for_termination()
