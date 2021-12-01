import orjson as _orjson
from typing import Any

class OrJson:
    @classmethod
    def dumps(cls, obj, *args, default: Any = None, **kwargs):
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return _orjson.loads(obj, *args, **kwargs)

__all__ = [
    'OrJson'
]