from typing import Any, Union
import msgpack
from msgpack import packb, unpackb


class Transformer:
    @staticmethod
    def encode(data: Any) -> Union[bytes, str]:
        """
        Encode data for Redis command
        - Strings and bytes are passed through unchanged
        - Numbers are converted to strings
        - Complex types are msgpacked
        """
        if isinstance(data, (str, bytes, float)):
            return data
        else:
            return packb(
                data,
                use_bin_type=True,
                strict_types=True,
                datetime=True
            )

    @staticmethod
    def decode(data: bytes) -> Any:
        """
        Decode Redis response
        - Try to unpack as msgpack first
        - If that fails, return as string or raw bytes
        """
        if not isinstance(data, bytes):
            return data

        try:
            return unpackb(
                data,
                raw=False,
                strict_map_key=False,
                use_list=False,
                timestamp=3
            )
        except msgpack.exceptions.ExtraData:
            # Data might be a simple string/bytes
            try:
                return data.decode()
            except UnicodeDecodeError:
                return data
