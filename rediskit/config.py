from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RedisConfig:
    host: str = 'localhost'
    port: int = 6379
    db: int = 0
    unix_socket: Optional[str] = None
    password: Optional[str] = None
    max_connections: int = 50
    socket_timeout: float = 5.0
    encoding: str = 'utf-8'
    buffer_size: int = 16384
    connection_retry_attempts: int = 3
    connection_retry_delay: float = 0.1
    tcp_keepalive: bool = True
    tcp_nodelay: bool = True
    tcp_quickack: bool = True
    tcp_fastopen: bool = True
