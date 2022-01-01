
import orjson as _orjson
import simdjson as _simdjson

from typing import Dict, Any, List, Union, overload
from os import PathLike
from pydantic import BaseModel
from .base import Srlzer, Mode

    
class JsonSrlzer(Srlzer):
    default_value: Dict[Any, Any] = None
    async_supported: bool = True
    cloud_supported: bool = True
    
    @classmethod
    def dumps(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return cls._encode(obj, *args, default = default, **kwargs)

    @classmethod
    def loads(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return cls._decode(obj, *args, **kwargs)
    
    @classmethod
    def _encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        raise NotImplementedError
        
    @classmethod
    def _decode(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        raise NotImplementedError
    
    @classmethod
    async def async_dumps(cls, obj: Dict[Any, Any], *args, default: Any = None, **kwargs) -> str:
        if not cls.async_supported: raise Exception
        return await cls._async_encode(obj, *args, default = default, **kwargs)

    @classmethod
    async def async_loads(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if not cls.async_supported: raise Exception
        return await cls._async_decode(obj, *args, **kwargs)
    
    @classmethod
    async def _async_encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        raise NotImplementedError
    
    @classmethod
    async def _async_decode(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        raise NotImplementedError

    @classmethod
    def loadlines(cls, path: PathLike, mode: str = Mode.read, encoding: str = 'utf-8', as_iterable: bool = False, ignore_errors: bool = True, *args, **kwargs):
        ## Change Later
        from pathlib import Path
        path = Path(path)
        if as_iterable:
            def jsonlines_iterator():
                with path.open(mode=mode, encoding=encoding) as f:
                    for line in f:
                        try: yield cls.loads(line, *args, **kwargs)
                        except StopIteration: break
                        except Exception as e:
                            if not ignore_errors: raise e
            return jsonlines_iterator                
        rez = []
        with path.open(mode=mode, encoding=encoding) as f:
            for line in f:
                try: rez.append(cls.loads(line, *args, **kwargs))
                except Exception as e:
                    if not ignore_errors: raise e
        return rez
    
    @classmethod
    def iterlines(cls, path: PathLike, mode: str = Mode.read, encoding: str = 'utf-8', ignore_errors: bool = True, *args, **kwargs):
        from pathlib import Path
        path = Path(path)
        with path.open(mode=mode, encoding=encoding) as f:
            for line in f:
                try: yield cls.loads(line, *args, **kwargs)
                except StopIteration: break
                except Exception as e:
                    if not ignore_errors: raise e



class OrJson(JsonSrlzer):
    
    @classmethod
    def _encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()
    
    @classmethod
    def _decode(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return _orjson.loads(obj, *args, **kwargs)
    
    @classmethod
    async def _async_encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()
    
    @classmethod
    async def _async_decode(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return _orjson.loads(obj, *args, **kwargs)


class SimdJson(JsonSrlzer):
    parser: _simdjson.Parser = _simdjson.Parser()
    
    @classmethod
    def _encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return _simdjson.dumps(obj, default=default, *args, **kwargs)
    
    @classmethod
    def _decode(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return _simdjson.loads(obj, *args, **kwargs)
    
    @classmethod
    def parse(cls, obj: Any, *args, **kwargs) -> Union[Union[_simdjson.Object, _simdjson.Array], Union[Dict[Any, Any], List[str]]]:
        if not isinstance(obj, (str, bytes)):
            if getattr(obj, 'content', None): return cls.parser.parse(obj.content, *args, **kwargs)
        return cls.parser.parse(obj, *args, **kwargs)
    
    @classmethod
    def decode(cls, obj: Any, *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if isinstance(obj, (dict, list, set)): return obj
        if issubclass(obj, BaseModel): return obj.dict()
        if issubclass(obj, _simdjson.Object) or isinstance(obj, _simdjson.Object): return obj.as_dict()
        if issubclass(obj, _simdjson.Array) or isinstance(obj, _simdjson.Array): return obj.as_list()
        if isinstance(obj, (str, bytes)): return _simdjson.loads(obj)
        raise ValueError
    
    @classmethod
    async def _async_encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return _simdjson.dumps(obj, default=default, *args, **kwargs)
    
    @classmethod
    async def _async_decode(cls, obj: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return _simdjson.loads(obj, *args, **kwargs)



    