import logging
import grpc
import time
import asyncio
from functools import wraps

from client.client.config import MAX_RETRIES, RETRY_DELAY

logger = logging.getLogger('socialnet.client.utils')


def retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """
    Decorator to retry a function on gRPC INTERNAL errors with exponential backoff.
    For other errors, returns None immediately without retrying.
    Supports both sync and async functions.
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except grpc.RpcError as e:
                    if e.code() != grpc.StatusCode.INTERNAL:
                        logger.error(f'{func.__name__} failed with {e.code()}: {e.details()}')
                        return None
                    
                    if attempt < max_retries - 1:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f'Attempt {attempt + 1} failed for {func.__name__}: INTERNAL error. Retrying in {wait_time}s...')
                        time.sleep(wait_time)
                    else:
                        logger.error(f'All {max_retries} attempts failed for {func.__name__}: INTERNAL error')
                        return None
            return None
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except grpc.RpcError as e:
                    if e.code() != grpc.StatusCode.INTERNAL:
                        logger.error(f'{func.__name__} failed with {e.code()}: {e.details()}')
                        return None
                    
                    if attempt < max_retries - 1:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f'Attempt {attempt + 1} failed for {func.__name__}: INTERNAL error. Retrying in {wait_time}s...')
                        time.sleep(wait_time)
                    else:
                        logger.error(f'All {max_retries} attempts failed for {func.__name__}: INTERNAL error')
                        return None
            return None
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    return decorator
