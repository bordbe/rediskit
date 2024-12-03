import pytest

from rediskit import RedisConfig


@pytest.fixture
def redis_config():
    return RedisConfig(
        host='localhost',
        port=6379,
        db=0,
    )
