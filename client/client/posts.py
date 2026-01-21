import logging
import grpc
import base64

from client.client.config import POST
from client.client.discoverer import get_host, get_authenticated_channel
from client.client.file_cache import FileCache
from client.client.utils import retry_on_failure

from protos.posts_pb2 import PostRequest, GetPostsRequest, GetPostRequest, RepostRequest, GetPostsIdRequest, GetPostsResponse, GetPostsIdResponse, GetPostResponse
from protos.posts_pb2_grpc import PostServiceStub
from protos import models_pb2

logger = logging.getLogger('socialnet.client.posts')
logger.setLevel(logging.INFO)

async def get_posts(username, token, request=True):
    if not request:
        cache = await FileCache.get(f'{username}_posts')
        if cache is not None:
            value = [models_pb2.Post.FromString(
                base64.b64decode(v)) for v in cache]
            response = GetPostsResponse(posts=value)
            return response
        else:
            logger.info(f'Posts of user {username} not found in cache')

    host = get_host(POST)
    channel = get_authenticated_channel(host, token)
    stub = PostServiceStub(channel)
    request = GetPostsRequest(user_id=username)

    try:
        response = stub.GetPosts(request)

        nposts = []
        for post in response.posts:
            new_post = models_pb2.Post(post_id=post.post_id, user_id=post.user_id, content=post.content,
                                       timestamp=post.timestamp, is_repost=post.is_repost, original_post_id=post.original_post_id)
            nposts.append(new_post)

        serialized_value = [base64.b64encode(
            v.SerializeToString()).decode('utf-8') for v in nposts]
        await FileCache.set(f'{username}_posts', serialized_value)
        return response
    except grpc.RpcError as e:
        logger.error(
            f'An error ocurred fetching user posts: {e.code()}: {e.details()}')

    logger.info(f'Recurring to cached user posts')
    cache = await FileCache.get(f'{username}_posts')
    if cache is not None:
        value = [models_pb2.Post.FromString(
            base64.b64decode(v)) for v in cache]
        response = GetPostsResponse(posts=value)
        return response
    else:
        logger.info(f'Posts of user {username} not found in cache')
        return None


async def get_posts_id(username, token, request=True):
    if not request:
        cache = await FileCache.get(f'{username}_posts_id')
        if cache is not None:
            value = [base64.b64decode(v) for v in cache]
            response = GetPostsIdRequest(posts_id=value)
            return response
        else:
            logger.info(f'Posts Id of user {username} not found in cache')

    host = get_host(POST)
    channel = get_authenticated_channel(host, token)
    stub = PostServiceStub(channel)
    request = GetPostsIdRequest(user_id=username)

    try:
        response = stub.GetPostsId(request)
        serialized_value = [base64.b64encode(v.encode('utf-8')).decode('utf-8') for v in response.posts_id]
        await FileCache.set(f'{username}_posts_id', serialized_value)
        return response
    except grpc.RpcError as e:
        logger.error(f'An error occurred fetching user posts Id: {e.code()} : {e.details()}')

    logger.info('Recurring to cache posts Id')

    cache = await FileCache.get(f'{username}_posts_id')
    if cache is not None:
        value = [base64.b64decode(v) for v in cache]
        response = GetPostsIdRequest(posts_id=value)
        return response
    else:
        logger.info(f'Posts Id of user {username} not found in cache')
        return None

async def get_post(post_id, token, request=False):
    if not request:
        cache = await FileCache.get(f'post_{post_id}')
        if cache is not None:
            value = models_pb2.Post.FromString(base64.b64decode(cache))
            response = GetPostResponse(post=value)
        else:
            logger.info(f'Post {post_id} not found in cache')
    
    host = get_host(POST)
    channel = get_authenticated_channel(host, token)
    stub = PostServiceStub(channel)
    request = GetPostRequest(post_id=post_id)

    try:
        response = stub.GetPost(request)
        serialized_value = base64.b64encode(response.post.SerializeToString()).decode('utf-8')
        await FileCache.set(f'posts/post_{post_id}', serialized_value)
        return response
    except grpc.RpcError as e:
        logger.error(f'An error ocurred fetching post: {e.code()}: {e.details()}')

    logger.info('Recurring to cache post')

    cache = await FileCache.get(f'post_{post_id}')
    if cache is not None:
        value = models_pb2.Post.FromString(base64.b64decode(cache))
        response = GetPostResponse(post=value)
    else:
        logger.info(f'Post {post_id} not found in cache')
        return None
    
@retry_on_failure()
def publish(username, content, token):
    host = get_host(POST)
    channel = get_authenticated_channel(host, token)
    stub = PostServiceStub(channel)
    request = PostRequest(user_id=username, content=content)
    
    response = stub.Publish(request)
    return response
    
@retry_on_failure()
def repost(username, original_post_id, token):
    host = get_host(POST)
    channel = get_authenticated_channel(host, token)
    stub = PostServiceStub(channel)
    request = RepostRequest(user_id=username, original_post_id=original_post_id)
    
    response = stub.Repost(request)
    return response