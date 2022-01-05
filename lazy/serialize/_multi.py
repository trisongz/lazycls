"""
Serializers that depend on/combine with one another
"""

__all__ = (
    'YamlBase64', 'YamlBGZ', 
    'JsonBase64', 'JasonBGZ'

)

from typing import Dict, Any, List, Union
from .core import Defaults

from ._json import Json
from ._yaml import Yaml
from ._base import Base


class YamlBase64(Yaml):
    
    @classmethod
    def dumps(cls, obj: Dict[Any, Any], dumper: str = Defaults.yaml_dumper, *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        rez = cls._encode(obj, dumper = dumper, *args, default = default, **kwargs)
        return Base.b64_encode(rez, *args, **kwargs)

    @classmethod
    def loads(cls, data: Union[str, bytes], loader: str = Defaults.yaml_loader, *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        rez = Base.b64_decode(data, *args, **kwargs)
        return cls._decode(rez, loader = loader, *args, **kwargs)
    
    @classmethod
    async def async_dumps(cls, obj: Dict[Any, Any], dumper: str = Defaults.yaml_dumper, *args, default: Any = None, **kwargs) -> str:
        if not cls.async_supported: raise Exception
        rez = await cls._async_encode(obj, dumper = dumper, *args, default = default, **kwargs)
        return Base.b64_encode(rez, *args, **kwargs)

    @classmethod
    async def async_loads(cls, data: Union[str, bytes], loader: str = Defaults.yaml_loader, *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if not cls.async_supported: raise Exception
        rez = Base.b64_decode(data, *args, **kwargs)
        return await cls._async_decode(rez, loader = loader, *args, **kwargs)

class YamlBGZ(Yaml):

    @classmethod
    def dumps(cls, obj: Dict[Any, Any], dumper: str = Defaults.yaml_dumper, *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        rez = cls._encode(obj, dumper = dumper, *args, default = default, **kwargs)
        return Base.b64_gzip_encode(rez, *args, **kwargs)

    @classmethod
    def loads(cls, data: Union[str, bytes], loader: str = Defaults.yaml_loader, *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        rez = Base.b64_gzip_decode(data, *args, **kwargs)
        return cls._decode(rez, loader = loader, *args, **kwargs)
    
    @classmethod
    async def async_dumps(cls, obj: Dict[Any, Any], dumper: str = Defaults.yaml_dumper, *args, default: Any = None, **kwargs) -> str:
        if not cls.async_supported: raise Exception
        rez = await cls._async_encode(obj, dumper = dumper, *args, default = default, **kwargs)
        return Base.b64_gzip_encode(rez, *args, **kwargs)

    @classmethod
    async def async_loads(cls, data: Union[str, bytes], loader: str = Defaults.yaml_loader, *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if not cls.async_supported: raise Exception
        rez = Base.b64_gzip_decode(data, *args, **kwargs)
        return await cls._async_decode(rez, loader = loader, *args, **kwargs)

class JsonBase64(Json):

    @classmethod
    def dumps(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        rez = cls._encode(obj, *args, default = default, **kwargs)
        return Base.b64_encode(rez, *args, **kwargs)

    @classmethod
    def loads(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        rez = Base.b64_decode(data, *args, **kwargs)
        return cls._decode(rez, *args, **kwargs)
    
    @classmethod
    async def async_dumps(cls, obj: Dict[Any, Any], *args, default: Any = None, **kwargs) -> str:
        if not cls.async_supported: raise Exception
        rez = await cls._async_encode(obj, *args, default = default, **kwargs)
        return Base.b64_encode(rez, *args, **kwargs)

    @classmethod
    async def async_loads(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if not cls.async_supported: raise Exception
        rez = Base.b64_decode(data, *args, **kwargs)
        return await cls._async_decode(rez, *args, **kwargs)


class JsonBGZ(Json):

    @classmethod
    def dumps(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        rez = cls._encode(obj, *args, default = default, **kwargs)
        return Base.b64_gzip_encode(rez, *args, **kwargs)

    @classmethod
    def loads(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        rez = Base.b64_gzip_decode(data, *args, **kwargs)
        return cls._decode(rez, *args, **kwargs)
    
    @classmethod
    async def async_dumps(cls, obj: Dict[Any, Any], *args, default: Any = None, **kwargs) -> str:
        if not cls.async_supported: raise Exception
        rez = await cls._async_encode(obj, *args, default = default, **kwargs)
        return Base.b64_gzip_encode(rez, *args, **kwargs)

    @classmethod
    async def async_loads(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if not cls.async_supported: raise Exception
        rez = Base.b64_gzip_decode(data, *args, **kwargs)
        return await cls._async_decode(rez, *args, **kwargs)