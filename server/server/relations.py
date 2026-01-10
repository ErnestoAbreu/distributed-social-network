import logging
import os
import grpc

from concurrent import futures

from server.server.auth import AuthRepository
from protos.models_pb2 import UserFollowing, UserFollowers
from protos.relations_pb2 import FollowResponse, UnfollowResponse, GetFollowingResponse, GetFollowersResponse
from protos.relations_pb2_grpc import RelationsServiceServicer, add_RelationsServiceServicer_to_server

logger = logging.getLogger('socialnet.server.relations')
logger.setLevel(logging.INFO)


class RelationsRepository:
    def __init__(self, node):
        self.node = node

    def load_following(self, username):
        path = os.path.join('User', username.lower(), 'Following')
        user_following, error = load(self.node, path, UserFollowing())

        following = []

        if error == grpc.StatusCode.NOT_FOUND:
            return following, None

        if error:
            logger.error(f'Failed to load following list {error}')
            return None, grpc.StatusCode.INTERNAL

        for user_id in user_following.following:
            following.append(user_id)

        return following, None

    def load_followers(self, username):
        path = os.path.join('User', username.lower(), 'Followers')
        user_followers, error = load(self.node, path, UserFollowers())

        followers = []

        if error == grpc.StatusCode.NOT_FOUND:
            return followers, None

        if error:
            logger.error(f'Failed to load followers list {error}')
            return None, grpc.StatusCode.INTERNAL

        for user_id in user_followers.followers:
            followers.append(user_id)

        return followers, None

    def add_to_following(self, username, followed_username):
        path = os.path.join('User', username.lower(), 'Following')
        user_following, error = self.load_following(username)

        following = []
        if not error:
            following = user_following

        if followed_username not in following:
            following.append(followed_username)

            error = save(self.node, UserFollowing(following=following), path)

            if error:
                logger.error(f'Failed to save following list {error}')
                return False, grpc.StatusCode.INTERNAL

            return True, None

        return False, None

    def remove_from_following(self, username, unfollowed_username):
        path = os.path.join('User', username.lower(), 'Following')
        user_following, error = self.load_following(username)

        following = []
        if not error:
            following = user_following

        if unfollowed_username in following:
            following.remove(unfollowed_username)

            error = save(self.node, UserFollowing(following=following), path)

            if error:
                logger.error(f'Failed to save following list {error}')
                return False, grpc.StatusCode.INTERNAL

            return True, None

        return False, None

    def add_to_followers(self, username, follower_username):
        path = os.path.join('User', username.lower(), 'Followers')
        user_followers, error = self.load_followers(username)

        followers = []
        if not error:
            followers = user_followers

        if follower_username not in followers:
            followers.append(follower_username)

            error = save(self.node, UserFollowers(followers=followers), path)

            if error:
                logger.error(f'Failed to save followers list {error}')
                return False, grpc.StatusCode.INTERNAL

            return True, None

        return False, None

    def remove_from_followers(self, username, unfollower_username):
        path = os.path.join('User', username.lower(), 'Followers')
        user_followers, error = self.load_followers(username)

        followers = []
        if not error:
            followers = user_followers

        if unfollower_username in followers:
            followers.remove(unfollower_username)

            error = save(self.node, UserFollowers(followers=followers), path)

            if error:
                logger.error(f'Failed to save followers {error}')
                return False, grpc.StatusCode.INTERNAL

            return True, None

        return False, None


class RelationsService(RelationsServiceServicer):
    def __init__(self, relations_repo: RelationsRepository, auth_repo: AuthRepository):
        self.relations_repo = relations_repo
        self.auth_repo = auth_repo

    def Follow(self, request, context):
        username = request.follower_id
        followed_username = request.followed_id

        if username == followed_username:
            return FollowResponse(success=False, message='Cannot follow yourself')

        exists, error = self.auth_repo.exists_user(username)
        if not exists:
            return FollowResponse(success=False, message='User not found')

        exists, error = self.auth_repo.exists_user(followed_username)
        if not exists:
            return FollowResponse(success=False, message='User to follow not found')

        ok, error = self.relations_repo.add_to_following(
            username, followed_username)
        if error:
            context.abort(grpc.StatusCode.INTERNAL,
                          f'Failed to follow user {followed_username}: {error}')

        if not ok:
            return FollowResponse(success=False, message=f'User {username} already follows {followed_username}')

        ok, error = self.relations_repo.add_to_followers(
            followed_username, username)
        if error:
            context.abort(grpc.StatusCode.INTERNAL,
                          f'Failed to follow user {followed_username}: {error}')

        if not ok:
            return FollowResponse(success=False, message=f'User {username} already follows {followed_username}')

        return FollowResponse(success=True, message=f'User {username} now is following {followed_username}')

    def Unfollow(self, request, context):
        username = request.follower_id
        unfollowed_username = request.unfollowed_id

        if username == unfollowed_username:
            return UnfollowResponse(success=False, message='Cannot unfollow yourself')

        exists, error = self.auth_repo.exists_user(username)
        if not exists:
            return UnfollowResponse(success=False, message='User not found')

        exists, error = self.auth_repo.exists_user(unfollowed_username)
        if not exists:
            return UnfollowResponse(success=False, message='User to unfollow not found')

        ok, error = self.relations_repo.remove_from_following(
            username, unfollowed_username)
        if error:
            context.abort(grpc.StatusCode.INTERNAL,
                          f'Failed to unfollow user {unfollowed_username}: {error}')

        if not ok:
            return UnfollowResponse(success=False, message=f'User {username} not follows {unfollowed_username}')

        ok, error = self.relations_repo.remove_from_followers(
            unfollowed_username, username)
        if error:
            context.abort(grpc.StatusCode.INTERNAL,
                          f'Failed to unfollow user {unfollowed_username}: {error}')

        if not ok:
            return UnfollowResponse(success=False, message=f'User {username} not follows {unfollowed_username}')

        return UnfollowResponse(success=True, message=f'User {username} is not following {unfollowed_username} anymore')

    def GetFollowing(self, request, context):
        username = request.user_id

        exists, error = self.auth_repo.exists_user(username)
        if not exists:
            context.abort(grpc.StatusCode.NOT_FOUND, 'User not found')

        following, error = self.relations_repo.load_following(username)
        if error:
            context.abort(grpc.StatusCode.INTERNAL,
                          f'Failed to load following list: {error}')

        return GetFollowingResponse(following=following)

    def GetFollowers(self, request, context):
        username = request.user_id

        exists, error = self.auth_repo.exists_user(username)
        if not exists:
            context.abort(grpc.StatusCode.NOT_FOUND, 'User not found')

        followers, error = self.relations_repo.load_followers(username)
        if error:
            context.abort(grpc.StatusCode.INTERNAL,
                          f'Failed to load followers list: {error}')

        return GetFollowersResponse(followers=followers)


def start_relations_service(addr, relations_repo: RelationsRepository, auth_repo: AuthRepository, max_workers: int = 10):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    add_RelationsServiceServicer_to_server(
        RelationsService(relations_repo, auth_repo), server)
    server.add_insecure_port(addr)
    server.start()
    port = str(addr).split(':')
    logger.info(f'Relations service started on port {port[1]}')
    server.wait_for_termination()
