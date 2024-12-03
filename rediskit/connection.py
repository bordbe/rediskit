from __future__ import annotations
from typing import Optional, Any, Set
from hiredis import Reader, pack_command
import socket
import asyncio
from contextlib import asynccontextmanager

from .config import RedisConfig
from .exceptions import ConnectionError
from .transformer import Transformer
from .utils import SingletonMeta


class RedisConnection:
    """low-level connection to Redis
    """

    def __init__(self, config: RedisConfig):
        self.host = config.host
        self.port = config.port
        self.password = config.password
        self.db = config.db
        self.socket_timeout = config.socket_timeout
        self.buffer_size = config.buffer_size

        self._parser = Reader()
        self._stream_reader: Optional[asyncio.StreamReader] = None
        self._stream_writer: Optional[asyncio.StreamWriter] = None

    def __await__(self):
        return self.init().__await__()

    async def init(self) -> RedisConnection:
        """
        Lazily initialize the Redis connection.
        """
        return self

    async def connect(self):
        """
        establish raw connection
        """
        try:
            self._stream_reader, self._stream_writer = await asyncio.open_connection(
                self.host,
                self.port,
            )
            sock = self._stream_writer.get_extra_info('socket')
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if hasattr(socket, 'TCP_KEEPIDLE'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)

            if self.password:
                await self._send_command('AUTH', self.password)
                response = await self._read_response()
                if response != b'OK':
                    raise ConnectionError("Authentication failed")

            if self.db != 0:
                await self._send_command('SELECT', str(self.db))
                response = await self._read_response()
                if response != b'OK':
                    raise ConnectionError(
                        f"Failed to select database {self.db}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect: {e}")

    async def _send_command(self, *args):
        """
        send raw command
        """
        packed_cmd = pack_command(args)
        self._stream_writer.write(packed_cmd)
        await self._stream_writer.drain()

    async def _read_response(self):
        """
        read response using hiredis parser
        """
        while True:
            response = self._parser.gets()
            if response is not False:
                return response
            data = await self._stream_reader.read(self.buffer_size)
            if not data:
                raise ConnectionError("Connection closed")
            print(data)
            self._parser.feed(data)

    async def execute_command(self, *args) -> Any:
        """
        execute command and return response
        """
        print("execuing command", *args)
        await self._send_command(*args)
        return await self._read_response()

    async def close(self):
        """
        close connection
        """
        if self._stream_writer:
            self._stream_writer.close()
            await self._stream_writer.wait_closed()


class RedisConnectionPool(metaclass=SingletonMeta):
    """
    custom connection pool for raw connections
    """

    def __init__(self, config: RedisConfig, max_size: int = 10):
        self.config = config
        self.max_size = max_size
        self._pool: asyncio.Queue[RedisConnection] = asyncio.Queue(max_size)
        self._in_use: Set[RedisConnection] = set()

    @asynccontextmanager
    async def connection(self):
        """context manager for acquiring and releasing connections"""
        conn = await self.acquire()
        try:
            yield conn
        finally:
            await self.release(conn)

    async def acquire(self) -> RedisConnection:
        """get connection from pool or create new one"""
        try:
            conn = self._pool.get_nowait()
        except asyncio.QueueEmpty:
            if len(self._in_use) < self.max_size:
                conn = await RedisConnection(self.config)
                await conn.connect()
            else:
                conn = await self._pool.get()
        self._in_use.add(conn)
        return conn

    async def release(self, conn: RedisConnection):
        """return connection to pool"""
        if conn not in self._in_use:
            return
        self._in_use.remove(conn)
        try:
            self._pool.put_nowait(conn)
        except asyncio.QueueFull:
            await conn.close()

    async def close(self):
        """close all connections"""
        self._closed = True

        for conn in self._in_use:
            await conn.close()
        self._in_use.clear()

        while not self._pool.empty():
            conn = await self._pool.get()
            await conn.close()

    @property
    def size(self) -> int:
        """current number of connections"""
        return len(self._in_use) + self._pool.qsize()

    @property
    def available(self) -> int:
        """number of available connections"""
        return self._pool.qsize()
