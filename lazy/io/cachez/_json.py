
import orjson as _orjson
#import ujson as _ujson
import simdjson as _simdjson
#import rapidjson as _rjson

from .static import *
from .core import Disk, JSONDisk, _zlib

_parser = _simdjson.Parser()

## OrJSON is only stable for non-float/int types like strings
## Since we already use orjson, we include this

class OrJSONDisk(Disk):
    "Cache key and value using JSON serialization with _zlib compression."

    def __init__(self, directory, compress_level=5, **kwargs):
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
        if not read:
            data = _orjson.loads(_zlib.decompress(data))
        return data

