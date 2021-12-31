import os
from pydantic.types import *
from lazycls.types import *
from lazycls.prop import classproperty
from lazycls.serializers import Base, OrJson, Yaml
from lazycls.ext.pathio import Path
from lazycls.ext._imports import LazyLib

""" For custom types """
# https://pydantic-docs.helpmanual.io/usage/types/#classes-with-__get_validators__

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
    def cast(cls, v: str): return Base.b64_decode(v)

class Base64Gzip(EnvType):
    """
    Returns Base64 + Gzip Encoded Strings
    """
    @classmethod
    def cast(cls, v: str): return Base.b64_gzip_decode(v)


class YamlStr(EnvType):
    """
    Returns Yaml.loads(str)
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): return Yaml.loads(v)


class PathStr(EnvType):
    """
    Returns Path.get_path(str, resolve=True)
    """
    @classproperty
    def default_value(cls): return None
    
    @classmethod
    def cast(cls, v: str): return Path.get_path(v, resolve=True)


class JsonB64Str(EnvType):
    """
    Returns Json.loads(Base.b64_decode(str))
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): 
        return OrJson.loads(Base.b64_decode(v))

class JsonB64GZipStr(EnvType):
    """
    Returns Json.loads(Base.b64_gzip_decode(str))
    """
    @classproperty
    def default_value(cls): return {}
    
    @classmethod
    def cast(cls, v: str): 
        return OrJson.loads(Base.b64_gzip_decode(v))


class AuthzFileStr(EnvType):
    """
    Returns Path to Credentials File
    """
    @classproperty
    def to_env_key(cls): return 'AUTHZ_CREDENTIALS'
    @classproperty
    def to_env_default_value(cls): return None
    @classproperty
    def to_env_path(cls):
        if LazyLib.is_avail_colab: return Path.get_path('/authz/auth_file.txt')
        return Path.cwd().joinpath('authz', 'auth_file.txt')
    
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
        if cur_env_val and Path(cur_env_val).exists() and not cls.override_env_to_value: return cur_env_val
        data = cls.dump_decoded(v)
        if data: return cls.write_to_file(data)
        return cur_env_val

class GoogleAuthJsonStr(AuthzFileStr):
    """
    Returns Path to GOOGLE_APPLICATION_CREDENTIALS
    """
    @classproperty
    def to_env_key(cls): return 'GOOGLE_APPLICATION_CREDENTIALS'
    @classproperty
    def to_env_path(cls):
        if LazyLib.is_avail_colab: return Path.get_path('/authz/adc.json')
        return Path.cwd().joinpath('authz', 'adc.json')
    @classmethod
    def cast(cls, v: str): return OrJson.loads(v)
    @classmethod
    def dump_decoded(cls, v): return OrJson.dumps(v)
    
class GoogleAuthB64(GoogleAuthJsonStr):
    """
    Returns Path to GOOGLE_APPLICATION_CREDENTIALS
    """
    
    @classmethod
    def cast(cls, v: str): return OrJson.loads(Base.b64_decode(v))
    
class GoogleAuthBGZ(GoogleAuthJsonStr):
    """
    Returns Path to GOOGLE_APPLICATION_CREDENTIALS
    """
    @classmethod
    def cast(cls, v: str): return OrJson.loads(Base.b64_gzip_decode(v))






__all__ = [
    'NoneStr',
    'NoneBytes',
    'StrBytes',
    'NoneStrBytes',
    'StrictStr',
    'ConstrainedBytes',
    'conbytes',
    'ConstrainedList',
    'conlist',
    'ConstrainedSet',
    'conset',
    'ConstrainedStr',
    'constr',
    'PyObject',
    'ConstrainedInt',
    'conint',
    'PositiveInt',
    'NegativeInt',
    'NonNegativeInt',
    'NonPositiveInt',
    'ConstrainedFloat',
    'confloat',
    'PositiveFloat',
    'NegativeFloat',
    'NonNegativeFloat',
    'NonPositiveFloat',
    'ConstrainedDecimal',
    'condecimal',
    'UUID1',
    'UUID3',
    'UUID4',
    'UUID5',
    'FilePath',
    'DirectoryPath',
    'Json',
    'JsonWrapper',
    'SecretStr',
    'SecretBytes',
    'StrictBool',
    'StrictBytes',
    'StrictInt',
    'StrictFloat',
    'PaymentCardNumber',
    'ByteSize',
    'List', 
    'Dict', 
    'Optional', 
    'Callable', 
    'TypeVar', 
    'Any', 
    'Union', 
    'Coroutine', 
    'Generator', 
    'Type', 
    'Tuple',
    'Base64', 
    'Base64Gzip', 
    'YamlStr', 
    'DictStr',
    'ListStr',
    'PathStr',
    'JsonB64Str',
    'JsonB64GZipStr',
    'GoogleAuthJsonStr',
    'GoogleAuthB64',
    'GoogleAuthBGZ',
]