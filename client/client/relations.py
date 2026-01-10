import logging
import grpc
import base64

from client.client.constants import *
from client.client.file_cache import FileCache
from client.client.discoverer import get_host, get_authenticated_channel

from protos.relations_pb2 import FollowRequest, UnfollowRequest, GetFollowersRequest, GetFollowingRequest
from protos.relations_pb2_grpc import RelationsServiceStub

logger = logging.getLogger('socialnet.client.relations')
logger.setLevel(logging.INFO)

def follow_user(follower_id, followed_id, token):
    host = get_host(RELATIONS)
    channel = get_authenticated_channel(host, token)
    stub = RelationsServiceStub(channel)
    request = FollowRequest(follower_id=follower_id, followed_id=followed_id)

    try:
        response = stub.Follow(request)
        return response
    except grpc.RpcError as e:
        logger.error(f'An error occurred following the user {e.code()}: {e.details()}')
        return False
    
def unfollow_user(follower_id, followed_id, token):
    host = get_host(RELATIONS)
    channel = get_authenticated_channel(host, token)
    stub = RelationsServiceStub(channel)
    request = UnfollowRequest(follower_id=follower_id, followed_id=followed_id)

    try:
        response = stub.Unfollow(request)
        return response
    except grpc.RpcError as e:
        logger.error(f'An error ocurred unfollowing the user: {e.code()} {e.details()}')
        return False

async def get_followers(username, token, request = True):
    if not request:
        cache = await FileCache.get(f'{username}_followers')
        if cache is not None:
            value = [base64.b64decode(v) for v in cache]
            return value
        else:
            logger.info(f'Followers of user {username} not found in cache')
    
    host = get_host(RELATIONS)
    channel = get_authenticated_channel(host, token)
    stub = RelationsServiceStub(channel)
    request = GetFollowersRequest(user_id=username)

    try:
        response = stub.GetFollowers(request)
        serialized_value = [base64.b64encode(v.encode('utf-8')).decode('utf-8') for v in response.followers]
        await FileCache.set(f'{username}_followers', serialized_value)
        return response.followers
    except grpc.RpcError as e:
        logger.error(f'An error ocurred fetching the followers list {e.code()} : {e.details()}')

    logger.info('Recurring to cache followers')

    cache = await FileCache.get(f'{username}_followers')
    if cache is not None:
        value = [base64.b64decode(v) for v in cache]
        return value
    else:
        logger.info(f'Followers of user {username} not found in cache')
        return None
    
async def get_following(username, token, request=True):
    if not request:
        cache = await FileCache.get(f'{username}_following')
        if cache is not None:
            value = [base64.b64decode(v) for v in cache]
            return value
        else:
            logger.info(f'Following of user {username} not found in cache')

    host = get_host(RELATIONS)
    channel = get_authenticated_channel(host, token)
    stub = RelationsServiceStub(channel)
    request = GetFollowingRequest(user_id=username)

    try:
        response = stub.GetFollowing(request)
        serialized_value = [base64.b64encode(v.encode('utf-8')).decode('utf-8') for v in response.following]
        await FileCache.set(f'{username}_following', serialized_value)
        return response.following
    except grpc.RpcError as e:
        logger.error(f'An error occurred fetching the following list {e.code()}: {e.details()}')

    logger.info(f'Recurring to cached following list')

    cache = await FileCache.get(f'{username}_following')
    if cache is not None:
        value = [base64.b64decode(v) for v in cache]
        return value
    else:
        logger.info(f'Following of user {username} not found in cache')
        return None