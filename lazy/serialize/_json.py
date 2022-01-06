
import orjson as _orjson

from os import PathLike
from pydantic import BaseModel
from typing import Dict, Any, List, Union, overload
from .core import Serializer, Mode, logger
from ._pysimd import _simdjson, SimdObject, SimdArray, create_simdobj


class JsonBase(Serializer):
    default_value: Dict[Any, Any] = None
    async_supported: bool = True
    cloud_supported: bool = True
    
    @classmethod
    def dumps(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return cls._encode(obj, *args, default = default, **kwargs)

    @classmethod
    def loads(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return cls._decode(data, *args, **kwargs)
    
    @classmethod
    def _encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        raise NotImplementedError
        
    @classmethod
    def _decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        raise NotImplementedError
    
    @classmethod
    async def async_dumps(cls, obj: Dict[Any, Any], *args, default: Any = None, **kwargs) -> str:
        if not cls.async_supported: raise Exception
        return await cls._async_encode(obj, *args, default = default, **kwargs)

    @classmethod
    async def async_loads(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if not cls.async_supported: raise Exception
        return await cls._async_decode(data, *args, **kwargs)
    
    @classmethod
    async def _async_encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        raise NotImplementedError
    
    @classmethod
    async def _async_decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        raise NotImplementedError

    @classmethod
    def readlines(cls, filelike, as_iterable: bool = False, ignore_errors: bool = True, *args, **kwargs):
        if as_iterable:
            def jsonlines_iterator():
                for line in filelike:
                    try: yield cls.loads(line, *args, **kwargs)
                    except (StopIteration, KeyboardInterrupt, GeneratorExit): break
                    except Exception as e:
                        logger.error(e)
                        if not ignore_errors: raise e
            return jsonlines_iterator
        rez = []
        for line in filelike:
            try: rez.append(cls.loads(line, *args, **kwargs))
            except Exception as e:
                logger.error(e)
                if not ignore_errors: raise e
        return rez
    
    @classmethod
    async def async_readlines(cls, filelike, as_iterable: bool = False, ignore_errors: bool = True, *args, **kwargs):
        if as_iterable:
            async def jsonlines_iterator():
                for line in filelike:
                    try: yield await cls.async_loads(line, *args, **kwargs)
                    except (StopIteration, KeyboardInterrupt, GeneratorExit): break
                    except Exception as e:
                        logger.error(e)
                        if not ignore_errors: raise e
            return jsonlines_iterator
        rez = []
        for line in filelike:
            try: rez.append(await cls.async_loads(line, *args, **kwargs))
            except Exception as e:
                logger.error(e)
                if not ignore_errors: raise e
        return rez
    
    @classmethod
    def loadlines(cls, path: PathLike, mode: str = Mode.read, encoding: str = 'utf-8', as_iterable: bool = False, ignore_errors: bool = True, *args, **kwargs):
        from lazy.io import get_path
        p = get_path(path)
        if as_iterable:
            def jsonlines_iterator():
                with p.open(mode=mode, encoding=encoding) as f:
                    for line in f:
                        try: yield cls.loads(line, *args, **kwargs)
                        except (StopIteration, KeyboardInterrupt, GeneratorExit): break
                        except Exception as e:
                            logger.error(e)
                            if not ignore_errors: raise e
            return jsonlines_iterator                
        rez = []
        with p.open(mode=mode, encoding=encoding) as f:
            for line in f:
                try: rez.append(cls.loads(line, *args, **kwargs))
                except Exception as e:
                    logger.error(e)
                    if not ignore_errors: raise e
        return rez
    
    @classmethod
    async def async_loadlines(cls, path: PathLike, mode: str = Mode.read, encoding: str = 'utf-8', as_iterable: bool = False, ignore_errors: bool = True, *args, **kwargs):
        from lazy.io import get_path
        p = get_path(path)
        if as_iterable:
            async def jsonlines_iterator():
                async with p.async_open(mode=mode, encoding=encoding) as f:
                    for line in f:
                        try: yield await cls.async_loads(line, *args, **kwargs)
                        except (StopIteration, KeyboardInterrupt, GeneratorExit): break
                        except Exception as e:
                            logger.error(e)
                            if not ignore_errors: raise e
            return jsonlines_iterator                
        rez = []
        async with p.async_open(mode=mode, encoding=encoding) as f:
            for line in f:
                try: rez.append(await cls.async_loads(line, *args, **kwargs))
                except Exception as e:
                    logger.error(e)
                    if not ignore_errors: raise e
        return rez

    @classmethod
    def iterlines(cls, path: PathLike, mode: str = Mode.read, encoding: str = 'utf-8', ignore_errors: bool = True, *args, **kwargs):
        from lazy.io import get_path
        p = get_path(path)
        with p.open(mode=mode, encoding=encoding) as f:
            for line in f:
                try: yield cls.loads(line, *args, **kwargs)
                except StopIteration: break
                except Exception as e:
                    logger.error(e)
                    if not ignore_errors: raise e
    
    @classmethod
    async def async_iterlines(cls, path: PathLike, mode: str = Mode.read, encoding: str = 'utf-8', ignore_errors: bool = True, *args, **kwargs):
        from lazy.io import get_path
        p = get_path(path)
        async with p.async_open(mode=mode, encoding=encoding) as f:
            for line in f:
                try: yield await cls.async_loads(line, *args, **kwargs)
                except StopIteration: break
                except Exception as e:
                    logger.error(e)
                    if not ignore_errors: raise e



class OrJson(JsonBase):
    
    @classmethod
    def _encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()
    
    @classmethod
    def _decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return _orjson.loads(data, *args, **kwargs)
    
    @classmethod
    async def _async_encode(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()
    
    @classmethod
    async def _async_decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return _orjson.loads(data, *args, **kwargs)


class SimdJson(JsonBase):
    parser: _simdjson.Parser = _simdjson.Parser()
    parser_enabled: bool = True
    
    @classmethod
    def _encode(cls, obj: Any, *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        if isinstance(obj, (SimdObject, SimdArray)): obj = obj.data
        return _simdjson.dumps(obj, default=default, *args, **kwargs)
    
    @classmethod
    def _decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if cls.parser_enabled: return create_simdobj(cls.parser.parse(data, *args, **kwargs))
        return _simdjson.loads(data, *args, **kwargs)
    
    @classmethod
    def parse(cls, data: Any, *args, **kwargs) -> Union[Union[_simdjson.Object, _simdjson.Array], Union[Dict[Any, Any], List[str]]]:
        if not isinstance(data, (str, bytes)) and getattr(data, 'content', None): return create_simdobj(cls.parser.parse(data.content, *args, **kwargs))
        return create_simdobj(cls.parser.parse(data, *args, **kwargs))
    
    @classmethod
    def decode(cls, data: Any, *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if isinstance(data, (dict, list, set)): return data
        if issubclass(data, BaseModel): return data.dict()
        #if issubclass(data, _simdjson.Object) or isinstance(data, _simdjson.Object): return data.as_dict()
        #if issubclass(data, _simdjson.Array) or isinstance(data, _simdjson.Array): return data.as_list()
        if isinstance(data, _simdjson.Object): return data.as_dict()
        if isinstance(data, _simdjson.Array): return data.as_list()
        if isinstance(data, (SimdObject, SimdArray)): return data.data
        if isinstance(data, (str, bytes)): return _simdjson.loads(data)
        raise ValueError
    
    @classmethod
    async def _async_encode(cls, obj: Any, *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        if isinstance(obj, (SimdObject, SimdArray)): obj = obj.data
        return _simdjson.dumps(obj, default=default, *args, **kwargs)
    
    @classmethod
    async def _async_decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if cls.parser_enabled: return create_simdobj(cls.parser.parse(data, *args, **kwargs))
        return _simdjson.loads(data, *args, **kwargs)



class Json(JsonBase):
    parser: _simdjson.Parser = SimdJson.parser
    parser_enabled: bool = True

    @staticmethod
    def parse(data: Any, *args, **kwargs) -> Union[Union[_simdjson.Object, _simdjson.Array], Union[Dict[Any, Any], List[str]]]:
        return SimdJson.parse(data, *args, **kwargs)
    
    @staticmethod
    def _encode(obj: Any, *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        if isinstance(obj, (SimdObject, SimdArray)): obj = obj.data
        return OrJson._encode(obj, default=default, *args, **kwargs)
    
    @classmethod
    def _decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if cls.parser_enabled: return SimdJson.parse(data, *args, **kwargs)
        return OrJson._decode(data, *args, **kwargs)
    
    @staticmethod
    async def _async_encode(obj: Any, *args, default: Dict[Any, Any] = None, **kwargs) -> str:
        if isinstance(obj, (SimdObject, SimdArray)): obj = obj.data
        return await OrJson._async_encode(obj, default=default, *args, **kwargs)
    
    @classmethod
    async def _async_decode(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        if cls.parser_enabled: return SimdJson.parse(data, *args, **kwargs)
        return await OrJson._async_decode(data, *args, **kwargs)