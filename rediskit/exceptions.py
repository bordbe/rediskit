class RedisKitError(Exception):
    """base exception for Redis kit"""
    pass


class ConnectionError(RedisKitError):
    """connection related errors"""
    pass


class SerializationError(RedisKitError):
    """serialization errors"""
    pass
