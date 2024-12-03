from typing import Dict


class SingletonMeta(type):
    """
    A metaclass for singleton pattern. 
    Classes using this metaclass will only have one instance.
    """
    _instances: Dict[type, object] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
