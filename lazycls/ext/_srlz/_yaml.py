import yaml

from typing import Dict, Any, List, Union, overload
from .base import Srlzer, Mode


class Yaml(Srlzer):
    default_value: Dict[Any, Any] = None
    async_supported: bool = True
    cloud_supported: bool = True
    
    
    @classmethod
    def dumps(cls, obj: Dict[Any, Any], dumper: str = 'default', *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return cls._encode(obj, dumper = dumper, *args, default = default, **kwargs)

    @classmethod
    def loads(cls, obj: Union[str, bytes], loader: str = 'default', *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return cls._decode(obj, loader = loader, *args, **kwargs)
    
    @classmethod
    def _encode(cls, obj: Dict[Any, Any], dumper: str = 'default', *args, default: Dict[Any, Any] = None, **kwargs) -> str: 
        _dumper = yaml.Dumper
        if dumper == 'safe': _dumper = yaml.SafeDumper
        return yaml.dump(obj, Dumper=_dumper *args, **kwargs)

    @classmethod
    def _decode(cls, obj: Union[str, bytes], loader: str = 'default', *args, **kwargs) -> Union[Dict[Any, Any], List[str]]: 
        _loader = yaml.Loader
        if loader == 'safe': _loader = yaml.SafeLoader
        return yaml.load(obj, Loader=_loader, *args, **kwargs)
    
    @classmethod
    async def async_dumps(cls, obj: Dict[Any, Any], dumper: str = 'default', *args, default: Any = None, **kwargs) -> str:
        if not cls.async_supported: raise Exception
        return await cls._async_encode(obj, dumper = dumper, *args, default = default, **kwargs)

    @classmethod
    async def async_loads(cls, obj: Union[str, bytes], loader: str = 'default', *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if not cls.async_supported: raise Exception
        return await cls._async_decode(obj, loader = loader, *args, **kwargs)
    
    @overload
    @classmethod
    async def _async_encode(cls, obj: Dict[Any, Any], dumper: str = 'default', *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        _dumper = yaml.Dumper
        if dumper == 'safe': _dumper = yaml.SafeDumper
        return yaml.dump(obj, Dumper=_dumper *args, **kwargs)
    
    @overload
    @classmethod
    async def _async_decode(cls, obj: Union[str, bytes], loader: str = 'default', *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        _loader = yaml.Loader
        if loader == 'safe': _loader = yaml.SafeLoader
        return yaml.load(obj, Loader=_loader, *args, **kwargs)