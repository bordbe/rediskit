
import pytest
from unittest.mock import AsyncMock, MagicMock
from rediskit.config import RedisConfig
from rediskit.connection import RedisConnection
from rediskit.exceptions import ConnectionError


@pytest.mark.asyncio
async def test_connection_init(redis_config):
    conn = RedisConnection(redis_config)
    assert conn.host == redis_config.host
    assert conn.port == redis_config.port
    assert conn.db == redis_config.db
