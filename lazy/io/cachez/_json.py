
import json as _defaultjson
import orjson as _orjson
import simdjson as _simdjson

from .static import *
from .base import Disk, _zlib
from .config import CachezConfigz

_parser = _simdjson.Parser()

## OrJSON is only stable for non-float/int types like strings
## Since we already use orjson, we include this

__all__ = ('JSONDisk', 'OrJSONDisk')

class JSONDisk(Disk):
    "Cache key and value using JSON serialization with zlib compression."

    def __init__(self, directory, compress_level: int = CachezConfigz.compression_lvl, **kwargs):
        """Initialize JSON disk instance.
        Keys and values are compressed using the zlib library. The
        `compress_level` is an integer from 0 to 9 controlling the level of
        compression; 1 is fastest and produces the least compression, 9 is
        slowest and produces the most compression, and 0 is no compression.
        :param str directory: directory path
        :param int compress_level: zlib compression level (default 1)
        :param kwargs: super class arguments
        """
        self.compress_level = compress_level
        super().__init__(directory, **kwargs)

    def put(self, key):
        json_bytes = _defaultjson.dumps(key).encode('utf-8')
        data = _zlib.compress(json_bytes, self.compress_level)
        return super().put(data)

    def get(self, key, raw):
        data = super().get(key, raw)
        return _defaultjson.loads(_zlib.decompress(data).decode('utf-8'))

    def store(self, value, read, key=UNKNOWN):
        if not read:
            json_bytes = _defaultjson.dumps(value).encode('utf-8')
            value = _zlib.compress(json_bytes, self.compress_level)
        return super().store(value, read, key=key)

    def fetch(self, mode, filename, value, read):
        data = super().fetch(mode, filename, value, read)
        if not read:
            data = _defaultjson.loads(_zlib.decompress(data).decode('utf-8'))
        return data


class OrJSONDisk(Disk):
    "Cache key and value using JSON serialization with _zlib compression."

    def __init__(self, directory, compress_level: int = CachezConfigz.compression_lvl, **kwargs):
        """Initialize JSON disk instance.
        Keys and values are compressed using the _zlib library. The
        `compress_level` is an integer from 0 to 9 controlling the level of
        compression; 1 is fastest and produces the least compression, 9 is
        slowest and produces the most compression, and 0 is no compression.
        :param str directory: directory path
        :param int compress_level: _zlib compression level (default 5)
        :param kwargs: super class arguments
        """
        self.compress_level = compress_level
        super().__init__(directory, **kwargs)

    def put(self, key):
        json_bytes = _orjson.dumps(key)
        data = _zlib.compress(json_bytes, self.compress_level)
        return super().put(data)

    def get(self, key, raw):
        data = super().get(key, raw)
        return _orjson.loads(_zlib.decompress(data))

    def store(self, value, read, key=UNKNOWN):
        if not read:
            json_bytes = _orjson.dumps(value)
            value = _zlib.compress(json_bytes, self.compress_level)
        return super().store(value, read, key=key)

    def fetch(self, mode, filename, value, read):
        data = super().fetch(mode, filename, value, read)
        if not read: data = _orjson.loads(_zlib.decompress(data))
        return data

