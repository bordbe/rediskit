from typing import Any
from contextlib import asynccontextmanager

from .connection import RedisConnectionPool
from .config import RedisConfig
from .transformer import Transformer

import logging

logger = logging.getLogger(__name__)


class AsyncClient:
    """
    Redis client
    """

    def __init__(self, config: RedisConfig):
        self._pool = RedisConnectionPool(config)
        self._decode = Transformer.decode
        self._encode = Transformer.encode

    async def execute(self, *args) -> Any:
        """Execute a single Redis command"""
        command = [self._encode(arg) for arg in args]
        async with self._pool.connection() as conn:
            response = await conn.execute_command(*command)
            # Decode response if it's bytes
            if isinstance(response, bytes):
                try:
                    return self._decode(response)
                except Exception as e:
                    # Return raw response if decoding fails
                    return response
            elif isinstance(response, (list, tuple)):
                # Handle array responses - decode each bytes element
                return [
                    self._decode(item) if isinstance(item, bytes) else item
                    for item in response
                ]

            # Return raw response for other types
            return response

    async def listen(self, channel: str, callback):
        """Listen for messages on a channel"""
        async with self._pool.connection() as conn:
            await conn.execute_command('SUBSCRIBE', channel)
            while True:
                response = await conn._read_response()
                logger.debug(f"response: {response}")
                if response and len(response) == 3 and response[0] == b'message':
                    try:
                        channel = response[1].decode()
                        message = self._decode(response[2])
                        await callback(message)
                    except Exception as e:
                        logger.error(e)
                        continue

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
            result = await self.execute('SETEX', key, ex, value)
        else:
            result = await self.execute('SET', key, value)
        return result == b'OK'

    async def get(self, key: str, default: Any = None) -> Any:
        """Get the value of key"""
        result = await self.execute('GET', key)
        if result is None:
            return default
        return result

    async def mset(self, mapping: dict) -> bool:
        """Set multiple keys to multiple values"""
        args = []
        for k, v in mapping.items():
            args.extend([k, v])
        result = await self.execute('MSET', *args)
        return result == b'OK'

    async def mget(self, keys: list) -> list:
        """Get the values of all the given keys"""
        results = await self.execute('MGET', *keys)
        return [
            item if item is None else item
            for item in results
        ]


class AsyncMessageBroker(AsyncClient):
    """
    Redis client with pub/sub operations
    """

    def __init__(self, config):
        super().__init__(config)

    async def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel"""
        result = await self.execute('PUBLISH', channel, message)
        return result

    async def subscribe(self, channel: str, callback):
        """Subscribe to channel"""
        await self.listen(channel, callback)
