import dill
import yaml

import base64
import gzip
import hashlib

import simdjson as _json
import orjson as _orjson
from typing import Any, Union

from .utils import Path, toPath, logger
from uuid import uuid4


class OrJson:
    binary: False

    @classmethod
    def dumps(cls, obj, *args, default: Any = None, **kwargs):
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return _orjson.loads(obj, *args, **kwargs)
    


class Json:
    p = _json.Parser()
    Object = _json.Object
    Array = _json.Array
    binary: False

    @classmethod
    def dumps(cls, obj, *args, **kwargs):
        return _json.dumps(obj, *args, **kwargs)

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return _json.loads(obj, *args, **kwargs)
    
    @classmethod
    def parse(cls, resp: Any, *args, **kwargs):
        return Json.p.parse(resp.content, *args, **kwargs)

    @classmethod
    def decode(cls, obj, *args, **kwargs):
        if isinstance(obj, dict): return obj
        if isinstance(obj, Any): return obj.dict()
        if isinstance(obj, _json.Object): return obj.as_dict()
        if isinstance(obj, _json.Array): return obj.as_list()
        if isinstance(obj, (str, bytes)): return Json.loads(obj)
        raise ValueError


class JsonCls:
    simd = Json
    orjson = OrJson

    @staticmethod
    def loadlines(path: Union[str, Path],  mode: str = 'r', encoding: str = 'utf-8', loader: str = 'simd', *args, **kwargs):
        path = toPath(path, resolve=True)
        loader = getattr(JsonCls, loader, JsonCls.simd)
        with path.open(mode=mode, encoding=encoding) as f:
            rez = [loader.loads(l, *args, **kwargs) for l in f]
        return rez
    
    @staticmethod
    def iterlines(path: Union[str, Path],  mode: str = 'r', encoding: str = 'utf-8', loader: str = 'simd', ignore_errors: bool = True, *args, **kwargs):
        path = toPath(path, resolve=True)
        loader = getattr(JsonCls, loader, JsonCls.simd)
        with path.open(mode=mode, encoding=encoding) as f:
            for l in f:
                try: yield loader.loads(l, *args, **kwargs)
                except Exception as e:
                    logger.error(e)
                    if ignore_errors: continue
                    raise

class Yaml:
    binary: False

    @classmethod
    def dumps(cls, obj, *args, **kwargs):
        return yaml.dump(obj, *args, **kwargs)

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return yaml.load(obj, Loader=yaml.Loader, *args, **kwargs)


class Pkl:
    binary: True

    @classmethod
    def dumps(cls, obj, *args, **kwargs):
        return dill.dumps(obj=obj, protocol=dill.HIGHEST_PROTOCOL, *args, **kwargs)

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return dill.loads(str=obj, *args, **kwargs)

class Base:
    encoding: str = "UTF-8"
    hash_method: str = "sha256"

    @classmethod
    def b64_encode(cls, text: str) -> str:
        return base64.b64encode(text.encode(encoding=cls.encoding)).decode(encoding=cls.encoding)

    @classmethod
    def b64_decode(cls, data: Union[str, bytes]) -> str:
        if isinstance(data, str): data = data.encode(encoding=cls.encoding)
        return base64.b64decode(data).decode(encoding=cls.encoding)

    @classmethod
    def b64_gzip_encode(cls, text: str) -> str:
        return base64.b64encode(gzip.compress(text.encode(encoding=cls.encoding))).decode(encoding=cls.encoding)

    @classmethod
    def b64_gzip_decode(cls, data: Union[str, bytes]) -> str:
        if isinstance(data, str): data = data.encode(encoding=cls.encoding)
        return gzip.decompress(base64.b64decode(data)).decode(encoding=cls.encoding)

    @classmethod
    def hash_encode(cls, text: str, method: str = 'sha256') -> str:
        encoder = getattr(hashlib, method)
        return encoder(text.encode(encoding=cls.encoding)).hexdigest()

    @classmethod
    def hash_compare(cls, text: str, hashtext: str, method: str = 'sha256') -> bool:
        return bool(cls.hash_encode(text, method) == hashtext)

    @classmethod
    def hash_b64_encode(cls, text: str, method: str = 'sha256') -> str:
        return cls.hash_encode(cls.b64_encode(text), method=method)

    @classmethod
    def hash_b64_gzip_encode(cls, text: str, method: str = 'sha256') -> str:
        return cls.hash_encode(cls.b64_gzip_encode(text), method=method)

    @classmethod
    def hash_b64_compare(cls, hashtext: str, data: Union[str, bytes], method: str = 'sha256') -> bool:
        if isinstance(data, str): return bool(cls.hash_b64_encode(text=data, method=method) == hashtext)
        return bool(cls.b64_decode(data) == hashtext)

    @classmethod
    def hash_b64_gzip_compare(cls, hashtext: str, data: Union[str, bytes], method: str = 'sha256') -> bool:
        if isinstance(data, str): return bool(cls.hash_b64_gzip_encode(text=data, method=method) == hashtext)
        return bool(cls.b64_gzip_decode(data) == hashtext)

    @classmethod
    def hash_b64_compare_match(cls, text: str, data: Union[str, bytes], method: str = 'sha256') -> bool:
        if isinstance(data, str): data = data.encode(encoding=cls.encoding)
        return bool(cls.b64_decode(data) == cls.hash_encode(text, method=method))

    @classmethod
    def hash_b64_gzip_compare_match(cls, text: str, data: Union[str, bytes], method: str = 'sha256') -> bool:
        if isinstance(data, str): data = data.encode(encoding=cls.encoding)
        return bool(cls.b64_gzip_decode(data) == cls.hash_encode(text, method=method))

    @staticmethod
    def get_uuid(*args, **kwargs):
        return str(uuid4(*args, **kwargs))

__all__ = [
    'Json',
    'OrJson',
    'JsonCls',
    'Yaml',
    'Pkl',
    'Base'
]