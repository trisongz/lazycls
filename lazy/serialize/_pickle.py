import sys
import gzip as _gzip
import zlib as _zlib
import bz2 as _bz2
import pickle as _pickle
import dill as _dill

from typing import Any
from .core import SerializerClsB

# If isal is available, use it over defaults
try:
    import isal
    from isal import igzip as _gzip
    from isal import isal_zlib as _zlib
    #_gzip = isal.igzip_lib
    #_zlib = isal.isal_zlib
except ImportError: pass

if sys.version_info.minor < 8:
    try:
        import pickle5 as _pickle
    except ImportError: pass

class DefaultProtocols:
    default: int = 4
    pickle: int = _pickle.HIGHEST_PROTOCOL
    dill: int = _dill.HIGHEST_PROTOCOL

class DefaultCompression:
    default: int = -1
    gzip: int = 10
    zlib: int = 6
    bz2: int = 9


class Compression:
    gzip = _gzip
    zlib = _zlib
    bz2 = _bz2


class BasePickle(SerializerClsB):
    default_value: Any = None
    async_supported: bool = True
    cloud_supported: bool = True
    compression_supported: bool = True
    
    @classmethod
    def dumps(cls, obj: Any, protocol: int = DefaultProtocols.default, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        return cls._encode(obj, protocol = protocol, compress = compress, *args, default = default, **kwargs)

    @classmethod
    def loads(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        return cls._decode(data, decompress = decompress, *args, **kwargs)
    
    @classmethod
    async def async_dumps(cls, obj: Any, protocol: int = DefaultProtocols.default, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        if not cls.async_supported: raise Exception
        return await cls._async_encode(obj, protocol = protocol, compress = compress , *args, default = default, **kwargs)

    @classmethod
    async def async_loads(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        if not cls.async_supported: raise Exception
        return await cls._async_decode(data, decompress = decompress, *args, **kwargs)
    
    @classmethod
    def compress(cls, data: bytes, method: str = 'gzip', compressionlvl: int = DefaultCompression.default, *args, **kwargs) -> bytes:
        _method = getattr(Compression, method)
        return _method.compress(data, compressionlvl = compressionlvl, *args, **kwargs)
    
    @classmethod
    def decompress(cls, data: bytes, method: str = 'gzip', *args, **kwargs) -> bytes:
        _method = getattr(Compression, method)
        return _method.decompress(data, *args, **kwargs)

    @classmethod
    def _encode(cls, obj: Any, protocol: int = DefaultProtocols.default, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        raise NotImplementedError
        
    @classmethod
    def _decode(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        raise NotImplementedError
    
    @classmethod
    async def _async_encode(cls, obj: Any, protocol: int = DefaultProtocols.default, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        raise NotImplementedError
    
    @classmethod
    async def _async_decode(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        raise NotImplementedError


class Pickle(BasePickle):
    @classmethod
    def _encode(cls, obj: Any, protocol: int = DefaultProtocols.pickle, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        data = _pickle.dumps(obj, protocol=protocol, *args, **kwargs)
        if compress: data = cls.compress(data, *args, **kwargs)
        return data
        
    @classmethod
    def _decode(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        if decompress: data = cls.decompress(data, *args, **kwargs)
        return _pickle.loads(data, *args, **kwargs)
          
    @classmethod
    async def _async_encode(cls, obj: Any, protocol: int = DefaultProtocols.pickle, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        data = _pickle.dumps(obj, protocol=protocol, *args, **kwargs)
        if compress: data = cls.compress(data, *args, **kwargs)
        return data
    
    @classmethod
    async def _async_decode(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        if decompress: data = cls.decompress(data, *args, **kwargs)
        return _pickle.loads(data, *args, **kwargs)


class Dill(BasePickle):
    @classmethod
    def _encode(cls, obj: Any, protocol: int = DefaultProtocols.dill, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        data = _dill.dumps(obj, protocol=protocol, *args, **kwargs)
        if compress: data = cls.compress(data, *args, **kwargs)
        return data
        
    @classmethod
    def _decode(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        if decompress: data = cls.decompress(data, *args, **kwargs)
        return _dill.loads(data, *args, **kwargs)
          
    @classmethod
    async def _async_encode(cls, obj: Any, protocol: int = DefaultProtocols.dill, compress: bool = False, *args, default: Any = None, **kwargs) -> bytes:
        data = _dill.dumps(obj, protocol=protocol, *args, **kwargs)
        if compress: data = cls.compress(data, *args, **kwargs)
        return data
    
    @classmethod
    async def _async_decode(cls, data: bytes, decompress: bool = False, *args, **kwargs) -> Any:
        if decompress: data = cls.decompress(data, *args, **kwargs)
        return _dill.loads(data, *args, **kwargs)

class Pkl(Dill):
    pass
