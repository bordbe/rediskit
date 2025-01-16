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
        self._pipe_mode = False
        self._pipe_commands = []

    async def execute(self, *args) -> Any:
        if self._pipe_mode:
            self._pipe_commands.append(args)
        async with self._pool.connection() as conn:
            response = await conn.execute_command(*args)
            return response

    @asynccontextmanager
    async def pipeline(self):
        """Pipeline multiple commands for batch execution"""
        self._pipe_mode = True
        try:
            yield self
        finally:
            async with self._pool.connection() as conn:
                for args in self._pipe_commands:
                    await conn.execute_command(*args)
            self._pipe_mode = False
            self._pipe_commands = []


class AsyncStorer(AsyncClient):
    """
    Redis client with storage operations
    """

    async def keys(self, pattern: str = '*') -> list[str]:
        result = await self.execute('KEYS', pattern)
        return [key.decode('utf-8') for key in result]

    async def set(self, key: str, value: Any) -> bool:
        result = await self.execute('SET', key, packb(value))
        return result == b'OK'

    async def get(self, key: str, default: Any = None) -> Any:
        result = await self.execute('GET', key)
        if result is None:
            return default
        return unpackb(result)

    async def setex(self, key: str, ex: int, value: Any) -> bool:
        result = await self.execute('SETEX', key, ex, packb(value))
        return result == b'OK'


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
