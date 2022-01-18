import os
import pathlib
from pydantic.types import *
from lazy.types import *
from lazy.libz import Lib
#from lazy.serialize import Serialize

__all__ = (
    'EnvType',
    'ListStr',
    'DictStr',
    'Base64',
    'Base64Gzip',
    'YamlStr',
    'PathStr',
    'AuthzDir',
    'JsonB64Str',
    'JsonB64GZipStr',
    'AuthzFileStr',
    'GoogleAuthJsonStr',
    'GoogleAuthB64',
    'GoogleAuthBGZ'
    
)

class EnvType(str):
    """
    Base Env Type that uses str
    """
    @classmethod
    def __get_validators__(cls): yield cls.validate
    
    @classmethod
    def cast(cls, v: str): return v
    
    @classmethod
    def cast_to_env(cls, v): return v
    
    
    @classproperty
    def default_value(cls): return ""
    @classproperty
    def list_delimiter(cls): return ','
    @classproperty
    def dict_delimiter(cls): return '='
    @classproperty
    def true_values(cls): return {'true', 'True', '1', 'yes', 'Yes'}
    @classproperty
    def false_values(cls): return {'false', 'False', '0', 'no', 'No'}
    @classproperty
    def none_values(cls): return {'', 'none', 'None', 'Null', 'null', 'n/a', 'N/A'}
    
    @classproperty
    def to_env_key(cls): return None
    @classproperty
    def to_env_default_value(cls): return None
    @classproperty
    def override_env_to_value(cls) -> bool: return False
    
    @classmethod
    def get_default_value(cls): return cls.default_value
    
    @classmethod
    def get_to_env_value(cls): 
        if not cls.to_env_key: return None
        return os.getenv(cls.to_env_key, cls.to_env_default_value)
    
    @classmethod
    def set_to_env(cls, to_val):
        if not to_val: return
        cur_env_val = cls.get_to_env_value()
        if cur_env_val and cur_env_val is not cls.to_env_default_value or cls.override_env_to_value:
            os.environ[cls.to_env_key] = to_val
    
    @classmethod
    def validate(cls, v):
        if not v: return cls.get_default_value()
        try: 
            val = cls.cast(v)
            if cls.to_env_key: 
                to_val = cls.cast_to_env(val)
                cls.set_to_env(to_val)
                return to_val
            return val
        except Exception as e: 
            #print(e)
            return ""
    
    @classmethod
    def __modify_schema__(cls, field_schema): pass
    
    def __repr__(self): return f'{self.__class__.__name__}({super().__repr__()})'
    

class ListStr(EnvType):
    """
    Returns List[str] by splitting on delimiter ','
    """
    @classproperty
    def default_value(cls): return []
    
    @classmethod
    def cast(cls, v: str):
        return v.split(cls.list_delimiter)
    
class DictStr(EnvType):
    """
    Returns List[str] by splitting on delimiter ','
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str):
        val = v.split(cls.list_delimiter)
        if not val: return cls.default_value
        rez = {}
        for i in val:
            for k,v in i.split(cls.dict_delimiter, 1):
                rez[k.strip()] = v.strip()
        return rez


class Base64(EnvType):
    """
    Returns Base64 Encoded Strings
    """
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.Base.b64_decode(v)

class Base64Gzip(EnvType):
    """
    Returns Base64 + Gzip Encoded Strings
    """
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.Base.b64_gzip_decode(v)


class YamlStr(EnvType):
    """
    Returns Yaml.loads(str)
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.Yaml.loads(v)


class YamlB64Str(EnvType):
    """
    Returns Yaml.loads(Base.b64_decode(str))
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.YamlB64.loads(v)


class YamlBGZStr(EnvType):
    """
    Returns Yaml.loads(Base.b64_gzip_decode(str))
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.YamlBGZ.loads(v)

#if TYPE_CHECKING:
#try: from lazy.io.pathz_v2 import PathLike, PathzLike, get_path
#except ImportError: 
PathLike = os.PathLike
PathzPath = Tuple[str, Type[pathlib.Path], Type[os.PathLike]]
PathzLike = Union[str, PathzPath]
get_path: Type[Callable] = None


#try: from lazy.io.pathz_v2 import PathLike, PathzPath
#except ImportError: 
#    PathLike = os.PathLike
#    PathzPath = Tuple[str, Type[pathlib.Path], Type[os.PathLike]]


def _get_pathio(p) -> 'PathzLike':
    #if get_path is None:
    #from lazy.io.pathz_v2.generic import get_path
    from lazy.io.pathz_v2.base import PathzPath
    return PathzPath(p)

"""
need to figure this out later. it's problematic. bc of type checking with CloudAuthz
"""

class PathStr(EnvType):
    """
    Returns lazy.io.get_path(str)
    """
    @classproperty
    def default_value(cls): return None
    

    @classmethod
    def cast(cls, v: str) -> 'PathLike':
        if '~' in v: v = v.replace('~', os.path.expanduser('~'))
        p = pathlib.Path(v)
        if p.is_dir(): p.mkdir(exist_ok=True, parents=True)
        return p
    
    @classmethod
    def validate(cls, v):
        if v is None: return cls.get_default_value()
        if not v: return cls.get_to_env_value()
        try: 
            val = cls.cast(v)
            if cls.to_env_key:
                to_val = cls.cast_to_env(val)
                cls.set_to_env(to_val)
                return to_val
            return val
        except Exception as e:
            #if not TYPE_CHECKING:
            #print('Error from configtypes', cls.__name__, e)
            return ""


class AuthzDir(EnvType):
    
    @classproperty
    def default_value(cls) -> 'PathLike':
        if Lib.is_avail_colab: pathlib.Path('/content/authz')
        if pathlib.Path('~/.authz').exists(): return pathlib.Path('~/.authz')
        return pathlib.Path.cwd().joinpath('.authz')

    @classmethod
    def cast(cls, v: str) -> 'PathLike':
        if '~' in v: v = v.replace('~', os.path.expanduser('~'))
        p = pathlib.Path(v)
        if p.is_dir(): p.mkdir(exist_ok=True, parents=True)
        return p
    

    @classmethod
    def validate(cls, v):
        if v is None: 
            v = cls.get_default_value()
            v.mkdir(exist_ok=True, parents=True)
            return v
        if not v: return cls.get_to_env_value()
        try: 
            val = cls.cast(v)
            if cls.to_env_key:
                to_val = cls.cast_to_env(val)
                cls.set_to_env(to_val)
                return to_val
            return val
        except Exception as e:
            return cls.get_default_value()



class _PathStr(EnvType):
    """
    Returns lazy.io.get_path(str)
    """
    @classproperty
    def default_value(cls): return None
    
    @classmethod
    def cast(cls, v: str) -> 'PathLike':
        #from lazy.io import get_path
        #from lazy.io.pathz_v2.generic import get_path
        # Fix Home
        if '~' in v: v = v.replace('~', os.path.expanduser('~'))
        ## will import dynamically later.
        #p = get_path(v)
        p = _get_pathio(v)
        p.resolve()
        if p.is_dir():
            p.mkdir(exist_ok=True, parents=True)
        return p
    
    @classmethod
    def validate(cls, v):
        if v is None: return cls.get_default_value()
        if not v: return cls.get_to_env_value()
        try: 
            val = cls.cast(v)
            if cls.to_env_key:
                to_val = cls.cast_to_env(val)
                cls.set_to_env(to_val)
                return to_val
            return val
        except Exception as e:
            #if not TYPE_CHECKING:
            #print('Error from configtypes', cls.__name__, e)
            return ""


class JsonB64Str(EnvType):
    """
    Returns Json.loads(Base.b64_decode(str))
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.JsonB64.loads(v)
        #return Serialize.Json.loads(Serialize.Base.b64_decode(v))

class JsonB64GZipStr(EnvType):
    """
    Returns Json.loads(Base.b64_gzip_decode(str))
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.JsonBGZ.loads(v)
        #return Serialize.Json.loads(Serialize.Base.b64_gzip_decode(v))


class AuthzFileStr(EnvType):
    """
    Returns Path to Credentials File
    """
    
    @classproperty
    def to_env_key(cls): return 'AUTHZ_CREDENTIALS'
    
    @classproperty
    def to_env_default_value(cls): return None
    
    @classproperty
    def to_env_path(cls) -> 'PathLike':
        if Lib.is_avail_colab: return pathlib.Path('/content/authz/auth_file.txt')
        return pathlib.Path(pathlib.Path.cwd().joinpath('authz', 'auth_file.txt').as_posix())
    
    @classmethod
    def dump_decoded(cls, v): return v
    
    @classmethod
    def write_to_file(cls, data):
        cls.to_env_path.parent.mkdir(exist_ok=True, parents=True)
        cls.to_env_path.write_text(data, encoding='utf-8') 
        return cls.to_env_path.string

    @classmethod
    def cast_to_env(cls, v):
        cur_env_val = cls.get_to_env_value()
        if cur_env_val and pathlib.Path(cur_env_val).exists() and not cls.override_env_to_value: return cur_env_val
        data = cls.dump_decoded(v)
        if data: return cls.write_to_file(data)
        return cur_env_val
    
    @classmethod
    def validate(cls, v):
        if v is None: return cls.get_default_value()
        if not v: return cls.get_to_env_value()
        try: 
            val = cls.cast(v)
            if cls.to_env_key:
                to_val = cls.cast_to_env(val)
                cls.set_to_env(to_val)
                return to_val
            return val
        except Exception as e: 
            print(e)
            return ""

"""
Cloud Providers
"""

"""
Google Cloud Platform
"""

class GoogleAuthJsonStr(AuthzFileStr):
    """
    Returns Path to GOOGLE_APPLICATION_CREDENTIALS
    """
    @classproperty
    def to_env_key(cls): return 'GOOGLE_APPLICATION_CREDENTIALS'
    
    @classproperty
    def to_env_path(cls):
        if Lib.is_avail_colab: return pathlib.Path('/content/authz/adc.json')
        return pathlib.Path(pathlib.Path.cwd().joinpath('authz', 'adc.json').as_posix())
    
    
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.Json.loads(v)
    
    @classmethod
    def dump_decoded(cls, v): 
        from lazy.serialize import Serialize
        return Serialize.Json.dumps(v)

class GoogleAuth(GoogleAuthJsonStr):
    """
    Returns Path to GOOGLE_APPLICATION_CREDENTIALS
    """
    pass

class GoogleAuthB64(GoogleAuthJsonStr):
    """
    Returns Path to GOOGLE_APPLICATION_CREDENTIALS
    """
    
    #@classmethod
    #def cast(cls, v: str): return Serialize.Json.loads(Serialize.Base.b64_decode(v))

    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.JsonB64.loads(v)
    
class GoogleAuthBGZ(GoogleAuthJsonStr):
    """
    Returns Path to GOOGLE_APPLICATION_CREDENTIALS
    """

    #@classmethod
    #def cast(cls, v: str): return Serialize.Json.loads(Serialize.Base.b64_gzip_decode(v))
    
    @classmethod
    def cast(cls, v: str): 
        from lazy.serialize import Serialize
        return Serialize.JsonBGZ.loads(v)
