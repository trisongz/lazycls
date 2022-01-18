import uuid
import gzip
import base64
import hashlib

from typing import Dict, Any, List, Union, overload
from .core import SerializerCls, Mode, Defaults

#DEFAULT_BASE_METHOD = 'base64'

class Base(SerializerCls):
    default_value: str = None
    async_supported: bool = True
    cloud_supported: bool = True
    base_method: str = Defaults.base_method
    hash_method: str = Defaults.hash_method

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
    def hash_encode(cls, text: str, method: str = Defaults.base_method) -> str:
        encoder = getattr(hashlib, method)
        return encoder(text.encode(encoding=cls.encoding)).hexdigest()

    @classmethod
    def hash_compare(cls, text: str, hashtext: str, method: str = Defaults.base_method) -> bool:
        return bool(cls.hash_encode(text, method) == hashtext)

    @classmethod
    def hash_b64_encode(cls, text: str, method: str = Defaults.base_method) -> str:
        return cls.hash_encode(cls.b64_encode(text), method=method)

    @classmethod
    def hash_b64_gzip_encode(cls, text: str, method: str = Defaults.base_method) -> str:
        return cls.hash_encode(cls.b64_gzip_encode(text), method=method)

    @classmethod
    def hash_b64_compare(cls, hashtext: str, data: Union[str, bytes], method: str = Defaults.base_method) -> bool:
        if isinstance(data, str): return bool(cls.hash_b64_encode(text=data, method=method) == hashtext)
        return bool(cls.b64_decode(data) == hashtext)

    @classmethod
    def hash_b64_gzip_compare(cls, hashtext: str, data: Union[str, bytes], method: str = Defaults.base_method) -> bool:
        if isinstance(data, str): return bool(cls.hash_b64_gzip_encode(text=data, method=method) == hashtext)
        return bool(cls.b64_gzip_decode(data) == hashtext)

    @classmethod
    def hash_b64_compare_match(cls, text: str, data: Union[str, bytes], method: str = Defaults.base_method) -> bool:
        if isinstance(data, str): data = data.encode(encoding=cls.encoding)
        return bool(cls.b64_decode(data) == cls.hash_encode(text, method=method))

    @classmethod
    def hash_b64_gzip_compare_match(cls, text: str, data: Union[str, bytes], method: str = Defaults.base_method) -> bool:
        if isinstance(data, str): data = data.encode(encoding=cls.encoding)
        return bool(cls.b64_gzip_decode(data) == cls.hash_encode(text, method=method))

    @staticmethod
    def get_uuid(method: str = Defaults.uuid_method, *args, **kwargs):
        t = getattr(uuid, method, Defaults.uuid_method)
        return str(t(*args, **kwargs))
    
    @staticmethod
    async def async_uuid(method: str = Defaults.uuid_method, *args, **kwargs):
        t = getattr(uuid, method, Defaults.uuid_method)
        return str(t(*args, **kwargs))
    
    @classmethod
    def get_encode_method(cls, method: str = Defaults.base_method):
        if method.lower() in {'b64', 'base64'}: return cls.b64_encode
        if method.lower() in {'bgz', 'base64gzip'}: return cls.b64_gzip_encode
        if method.lower() in {'hash', 'h'}: return cls.hash_encode
        raise NotImplementedError
    
    @classmethod
    def get_decode_method(cls, method: str = Defaults.base_method):
        if method.lower() in {'b64', 'base64'}: return cls.b64_decode
        if method.lower() in {'bgz', 'base64gzip'}: return cls.b64_gzip_decode
        raise NotImplementedError
    
    @classmethod
    async def async_dumps(cls, data: str, method: str = Defaults.base_method, *args, **kwargs):
        return await cls._async_encode(data, method = method, *args, **kwargs)

    @classmethod
    async def async_loads(cls, data: str, method: str = Defaults.base_method, *args, **kwargs):
        return await cls._async_decode(data, method = method, *args, **kwargs)
    
    @classmethod
    def dumps(cls, data: str, method: str = Defaults.base_method, *args, **kwargs) -> str:
        return cls._encode(data, method = method, *args, **kwargs)

    @classmethod
    def loads(cls, data: Union[str, bytes], method: str = Defaults.base_method, *args, **kwargs) -> str:
        return cls._decode(data, method = method, *args, **kwargs)
    
    @classmethod
    def _encode(cls, data: str, method: str = Defaults.base_method, *args,  **kwargs) -> str:
        _method = cls.get_encode_method(method)
        return _method(data, *args, **kwargs)

    @classmethod
    def _decode(cls, data: Union[str, bytes], method: str = Defaults.base_method, *args, **kwargs) -> str:
        _method = cls.get_decode_method(method)
        return _method(data, *args, **kwargs)
    
    @classmethod
    async def _async_encode(cls, data: str, method: str = Defaults.base_method, *args, **kwargs) -> str:
        _method = cls.get_encode_method(method)
        return _method(data, *args, **kwargs)
    
    @classmethod
    async def _async_decode(cls, data: Union[str, bytes], method: str = Defaults.base_method, *args, **kwargs) -> str:
        _method = cls.get_decode_method(method)
        return _method(data, *args, **kwargs)

        
    