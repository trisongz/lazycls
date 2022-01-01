__all__ = (
    'Srlzer',
    'SrlzerB',
    'Mode'
)

from typing import Any
from .static import Mode

class Srlzer:
    encoding: str = 'utf-8'
    binary: bool = False
    read_mode: str = Mode.read
    write_mode: str = Mode.write
    append_mode: str = Mode.append
    default_mode: str = Mode.read
    default_value: Any = None    
    cloud_supported: bool = False
    async_supported: bool = False
    
    @classmethod
    def _validate_inputs(cls, obj, *args, default: Any = None, **kwargs):
        raise NotImplementedError
    
    @classmethod
    def _validate_outputs(cls, value: Any, *args, **kwargs):
        raise NotImplementedError
    
    @classmethod
    def _encode(cls, obj, *args, default: Any = None, **kwargs):
        raise NotImplementedError
    
    @classmethod
    def _decode(cls, obj, *args, **kwargs):
        raise NotImplementedError
    
    @classmethod
    async def _async_encode(cls, obj, *args, default: Any = None, **kwargs):
        raise NotImplementedError
    
    @classmethod
    async def _async_decode(cls, obj, *args, **kwargs):
        raise NotImplementedError
    
    @classmethod
    def dumps(cls, obj, *args, default: Any = None, **kwargs):
        return cls._encode(obj, *args, default = default, **kwargs)

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return cls._decode(obj, *args, **kwargs)
    
    @classmethod
    async def async_dumps(cls, obj, *args, default: Any = None, **kwargs):
        if not cls.async_supported: raise Exception
        return await cls._async_encode(obj, *args, default = default, **kwargs)

    @classmethod
    async def async_loads(cls, obj, *args, **kwargs):
        if not cls.async_supported: raise Exception
        return await cls._async_decode(obj, *args, **kwargs)
    
class SrlzerB(Srlzer):
    encoding: str = None
    binary: bool = True
    read_mode: str = Mode.read_binary
    write_mode: str = Mode.write_binary
    append_mode: str = None
    default_mode: str = Mode.read_binary

