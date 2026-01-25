import logging
import os
import grpc
import time
from concurrent import futures

from server.server.auth import AuthRepository
from server.server.chord.core import load, save
from protos.models_pb2 import Post, UserPosts
from protos.posts_pb2 import PostResponse, GetPostsResponse, RepostResponse, GetPostsIdResponse, GetPostResponse
from protos.posts_pb2_grpc import PostServiceServicer, add_PostServiceServicer_to_server
from server.server.chord.node import ChordNode
from server.server.security import get_tls_config

logger = logging.getLogger('socialnet.server.posts')


class PostRepository:
    def __init__(self, node: ChordNode) -> None:
        self.node = node

    def create_post_key(self, post_id: str) -> str:
        return os.path.join('Post', post_id)
    
    def create_user_posts_key(self, username: str) -> str:
        return os.path.join('User', username.lower(), 'Posts')

    def save_post(self, post: Post) -> grpc.StatusCode | None:
        key = self.create_post_key(post.post_id)
        error = save(self.node, key, post)

        if error:
            logger.error(f'Failed to save post {error}')
            return grpc.StatusCode.INTERNAL
        
        error = self.add_to_posts_list(post.post_id, post.user_id)

        if error:
            logger.error(f'Failed toa add post to user list {error}')
            return grpc.StatusCode.INTERNAL

        return None

    def load_post(self, post_id: str) -> tuple[Post, grpc.StatusCode | None]:
        key = self.create_post_key(post_id)
        post, error = load(self.node, key, Post())

        if error == grpc.StatusCode.NOT_FOUND:
            return None, grpc.StatusCode.NOT_FOUND

        if error:
            return None, grpc.StatusCode.INTERNAL

        return post, None

    def add_to_posts_list(self, post_id, username):
        key = self.create_user_posts_key(username)
        user_posts, error = load(self.node, key, UserPosts())

        nposts = []
        if not error:
            nposts = user_posts.posts_id

        nposts.append(post_id)
        error = save(self.node, key, UserPosts(posts_id=nposts))

        if error:
            logger.error(f'Failed to save post {post_id} to user {username}: {error}')
            return grpc.StatusCode.INTERNAL

        return None

    def load_posts_list(self, username):
        key = self.create_user_posts_key(username)
        user_posts, error = load(self.node, key, UserPosts())

        posts = []
        if error == grpc.StatusCode.NOT_FOUND:
            return posts, None

        if error:
            logger.error(f'Failed to load user {username} posts: {error}')
            return grpc.StatusCode.INTERNAL, None

        for post_id in user_posts.posts_id:
            post, error = self.load_post(post_id)
            if error:
                return None, error
            posts.append(post)

        return posts, None

    def load_posts_id_list(self, username):
        key = self.create_user_posts_key(username)
        user_posts, error = load(self.node, key, UserPosts())

        posts = []
        if error == grpc.StatusCode.NOT_FOUND:
            return posts, None

        if error:
            logger.error(f'Failed to load user {username} posts: {error}')
            return grpc.StatusCode.INTERNAL, None

        for post_id in user_posts.posts_id:
            posts.append(post_id)

        return posts, None


class PostService(PostServiceServicer):
    def __init__(self, post_repo: PostRepository, auth_repo: AuthRepository):
        self.post_repo = post_repo
        self.auth_repo = auth_repo

    def GetPosts(self, request, context):
        user_id = request.user_id

        if not self.auth_repo.load_user(user_id):
            context.abort(grpc.StatusCode.NOT_FOUND, 'User not found')

        posts, error = self.post_repo.load_posts_list(user_id)

        if error:
            context.abort(grpc.StatusCode.INTERNAL, 'Failed to load user posts')

        return GetPostsResponse(posts=posts)

    def GetPostsId(self, request, context):
        user_id = request.user_id

        exists, error = self.auth_repo.exists_user(user_id)

        if not exists: 
            context.abort(grpc.StatusCode.NOT_FOUND, 'User not found')

        posts, error = self.post_repo.load_posts_id_list(user_id)

        if error:
            context.abort(grpc.StatusCode.INTERNAL, 'Failed to load user posts id')

        return GetPostsIdResponse(posts_id=posts)

    def GetPost(self, request, context):
        post_id = request.post_id

        post, error = self.post_repo.load_post(post_id)

        if error:
            context.abort(grpc.StatusCode.INTERNAL, 'Failed to get post')

        return GetPostResponse(post=post)

    def Publish(self, request, context):
        user_id = request.user_id
        content = request.content

        post_id = str(time.time_ns())
        iso_timestamp = self.post_repo.node.get_datetime()

        post = Post(post_id=post_id, user_id=user_id, content=content, timestamp=iso_timestamp, is_repost=False)

        error = self.post_repo.save_post(post)

        if error:
            context.abort(error, 'Failed to save post')

        return PostResponse(success=True, message='Post published successfully')

    def Repost(self, request, context):
        user_id = request.user_id
        original_post_id = request.original_post_id

        posts, error = self.post_repo.load_posts_list(user_id)

        for post in posts:
            if post.original_post_id == original_post_id:
                return RepostResponse(success=False, message='Already reposted')

            if post.post_id == original_post_id:
                return RepostResponse(success=False, message='You are the owner of the post')

        original_post, error = self.post_repo.load_post(original_post_id)

        if error:
            context.abort(grpc.StatusCode.NOT_FOUND, 'Original post not found')

        post_id = str(time.time_ns())
        iso_timestamp = self.post_repo.node.get_datetime()
        post = Post(post_id=post_id, user_id=user_id, content=original_post.content, timestamp=iso_timestamp, is_repost=True,
                    original_post_id=original_post.post_id, original_post_user_id=original_post.user_id, original_post_timestamp=original_post.timestamp)

        error = self.post_repo.save_post(post)

        if error:
            context.abort(grpc.StatusCode.INTERNAL, 'Failed to save repost')

        return RepostResponse(success=True, message='Post reposted successfully')


def start_post_service(addr, post_repo: PostRepository, auth_repo: AuthRepository, max_workers: int = 10):
    from server.server.config import USE_TLS
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_PostServiceServicer_to_server(PostService(post_repo, auth_repo), server)
    
    if not USE_TLS:
        server.add_insecure_port(addr)
        logger.info(f'Post service started on insecure port {addr} (TLS disabled)')
    else:
        credentials = get_tls_config().load_credentials()
        if credentials:
            server.add_secure_port(addr, credentials)
            logger.info(f'Post service started on secure port {addr} with mTLS')
        else:
            server.add_insecure_port(addr)
            logger.warning(f'Post service started on insecure port {addr} (TLS credentials failed)')
    
    server.start()
    server.wait_for_termination()