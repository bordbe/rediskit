import functools
import logging
from typing import Any, Callable, Optional, Union
from datetime import timedelta

from ..client import AsyncStorer

logger = logging.getLogger(__name__)


class redis_ttl_cache:
    """
    Caches the function's return value into Redis using a key generated with
    module_name, function_name and args.

    Args:
        storer (AsyncStorer): Redis storer instance for cache operations
        ttl (Union[int, timedelta]): Time to live for cached items in seconds or timedelta
        key_builder (Optional[Callable]): Custom function to build cache keys
    """

    def __init__(
        self,
        storer: 'AsyncStorer',
        ttl: Union[int, timedelta] = None,
        key_builder: Optional[Callable] = None,
    ):
        self.storer = storer
        self.key_builder = key_builder

        if isinstance(ttl, timedelta):
            self.ttl = int(ttl.total_seconds())
        else:
            self.ttl = ttl

    def __call__(self, func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await self.decorator(func, *args, **kwargs)
        wrapper.cache = self.storer
        return wrapper

    async def decorator(
        self, func, *args, cache_read=True, cache_write=True, aiocache_wait_for_write=True, **kwargs
    ):
        key = self.get_cache_key(func, args, kwargs)
        if cache_read:
            try:
                cached = await self.storer.get(key)
                if cached is not None:
                    return cached
            except Exception as e:
                logger.error(f"Error reading from cache: {str(e)}")
        result = await func(*args, **kwargs)
        if cache_write:
            try:
                await self.storer.setex(key, self.ttl, result)
            except Exception as e:
                logger.error(f"Error writing to cache: {str(e)}")
        return result

    def get_cache_key(self, func: Callable, args: tuple, kwargs: dict) -> str:
        """Generate a cache key based on function name and arguments."""
        if self.key_builder is not None:
            return self.key_builder(func, *args, **kwargs)
        ordered_kwargs = sorted(kwargs.items())
        return f"{func.__module__ or ''}{func.__qualname__}:{args}:{ordered_kwargs}"
