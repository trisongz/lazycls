import orjson as _orjson
import yaml
from typing import Any

class OrJson:
    @classmethod
    def dumps(cls, obj, *args, default: Any = None, **kwargs):
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return _orjson.loads(obj, *args, **kwargs)

class Yaml:
    @classmethod
    def dumps(cls, obj, *args, **kwargs):
        return yaml.dump(obj, *args, **kwargs)

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return yaml.load(obj, Loader=yaml.Loader, *args, **kwargs)

__all__ = [
    'OrJson',
    'Yaml'
]