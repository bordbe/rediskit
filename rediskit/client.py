from typing import Any
from contextlib import asynccontextmanager
from msgpack import packb, unpackb

from .connection import RedisConnectionPool
from .config import RedisConfig

import logging

logger = logging.getLogger(__name__)


class AsyncClient:
    """
    Redis client
    """

    def __init__(self, config: RedisConfig):
        self._pool = RedisConnectionPool(config)

    async def execute(self, *args) -> Any:
        async with self._pool.connection() as conn:
            response = await conn.execute_command(*args)
            return response

    @asynccontextmanager
    async def pipeline(self):
        """Pipeline multiple commands for batch execution"""
        self._pipeline_mode = True
        self._pipeline_commands = []
        try:
            yield self
        finally:
            self._pipeline_mode = False
            async with self._pool.connection() as conn:
                for cmd, args in self._pipeline_commands:
                    await conn.execute_command(cmd, *args)
            self._pipeline_commands = []


class AsyncStorer(AsyncClient):
    """
    Redis client with storage operations
    """

    async def set(self, key: str, value: Any, ex: int = None) -> bool:
        """Set key to hold the string value"""
        if ex:
            result = await self.execute('SETEX', key, ex, packb(value))
        else:
            result = await self.execute('SET', key, packb(value))
        return result == b'OK'

    async def get(self, key: str, default: Any = None) -> Any:
        """Get the value of key"""
        result = await self.execute('GET', key)
        if result is None:
            return default
        return unpackb(result)


class AsyncMessageBroker(AsyncClient):
    """
    Redis client with pub/sub operations
    """

    def __init__(self, config):
        super().__init__(config)

    async def listen(self, channel: str, callback):
        """Listen for messages on a channel"""
        async with self._pool.connection() as conn:
            await conn.execute_command('SUBSCRIBE', channel)
            while True:
                response = await conn._read_response()
                if response and len(response) == 3 and response[0] == b'message':
                    try:
                        message = unpackb(response[2])
                        await callback(message)
                    except Exception as e:
                        logger.error(e)
                        continue

    async def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel"""
        result = await self.execute('PUBLISH', channel, packb(message))
        return result

    async def subscribe(self, channel: str, callback):
        """Subscribe to channel"""
        await self.listen(channel, callback)
