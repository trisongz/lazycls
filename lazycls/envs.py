import os
from .prop import classproperty
from .serializers import OrJson, Yaml
from .funcs import doesTextMatch
from .utils import Path, toPath
from .types import *


def hasEnv(name: str) -> bool: return os.getenv(name, None) is not None

def getEnv(): return {k:v for k,v in os.environ.items()}

def envToBool(name: str, default: str = 'false'): return bool(os.getenv(name, default).lower() in {'true', '1', 'yes'})

def envToType(value: str = None, valType = str):
    if value is None or value in {'none', 'None'}: return None
    return valType(value)

def envToList(name: str, sep: str = ',', default: List[str] = []):
    d = os.getenv(name, None)
    if d: return d.split(sep)
    return default

def envToDict(name: str, sep: str = '=', default: Dict[str, Any] = {}):
    d = envToList(name)
    if not d: return default
    rez = {}
    for i in d:
        for k,v in i.split(sep, 1):
            rez[k] = v
    return rez

def envToFloat(name: str, default: float = 0.0):
    d = os.getenv(name, None)
    if d is None: return default
    return envToType(d, float)

def envToInt(name: str, default: int = 0):
    d = os.getenv(name, None)
    if d is None: return default
    return envToType(d, int)

def envInVals(name: str, checkVals: List[str] = [], exact: bool = False, **kwargs) -> bool:
    d = os.getenv(name, None)
    if d is None: return False
    return doesTextMatch(text=d, items=checkVals, exact=exact, **kwargs)

def envToStr(name: str, default: str = ''):
    d = os.getenv(name, None)
    if d is None: return default
    return d


def toEnv(name: str, value: Any, override: bool = False):
    if not hasEnv(name) or override: os.environ[name] = str(value)

_LoadedEnvFiles = set()

def loadEnvFile(path: Union[str, Path], override: bool = False):
    global _LoadedEnvFiles
    path = toPath(path, resolve=True)
    if not path.exists(): return None
    if path.name in _LoadedEnvFiles: return True
    loader = None
    if path.suffix == '.json': loader = OrJson.loads
    elif path.suffix == '.yaml': loader = Yaml.loads
    if loader is None: return False
    data = loader(path.read_text())
    for k,v in data.items():
        toEnv(name=k, value=v, override=override)
    _LoadedEnvFiles.add(path.name)
    return True

def setEnvDict(data: Dict[str, Any], override: bool = False):
    for k,v in data.items():
        toEnv(name=k, value=v, override=override)

load_env_file = loadEnvFile
to_env = toEnv
set_env_from_dict = setEnvDict

"""
New Class to encapsulate all the above functions, to allow a singular import
of lazycls.envs.Env

will later deprecate the above functions
"""

class Env:
    list_delimiter: str = ','
    dict_delimiter: str = '='


    @classproperty
    def dict(cls):
        return {k:v for k,v in os.environ.items()}
    
    @classproperty
    def keys(cls):
        return list(os.environ.keys())
    
    @classproperty
    def values(cls):
        return list(os.environ.values())
    
    @classmethod
    def has(cls, key: str) -> bool:
        return os.getenv(key, None) is not None

    @classmethod
    def get(cls, key: str, default: Any = None):
        return os.environ.get(key, default)
    
    @staticmethod
    def cast(value: str = None, val_type = str):
        if value is None or value in {'none', 'None'}: return None
        return val_type(value)
    
    @classmethod
    def to_bool(cls, key: str, default: str = 'false'):
        return bool(os.getenv(key, default).lower() in {'true', '1', 'yes'})

    @classmethod
    def to_str(cls, key: str, default: Any = ''):
        val = os.getenv(key, None)
        if val is None: return default
        return val
    
    @classmethod
    def to_str_env(cls, src_key: str, to_key: str, default: Any = '', override: bool = False):
        """ used to handle credentials like AWS Keys, etc without directly exposing known env keys
            i.e. map src_key = AWS_KEYID -> to_key = AWS_ACCESS_KEY_ID
        """
        val = cls.to_str(src_key, default)
        if val: cls.set_env(key=to_key, value=val, override=override)
        return val
    
    @classmethod
    def to_list(cls, key: str, default: List[str] = []) -> List[str]:
        val = os.getenv(key, None)
        if val: return val.split(cls.list_delimiter)
        return default
    
    @classmethod
    def to_dict(cls, key: str, default: Dict[str, Any] = {}) -> Dict[str, Any]:
        val = cls.to_list(key)
        if not val: return default
        rez = {}
        for i in val:
            for k,v in i.split(cls.dict_delimiter, 1):
                rez[k.strip()] = v.strip()
        return rez

    @classmethod
    def to_float(cls, key: str, default: float = 0.0):
        val = os.getenv(key, None)
        if val is None: return default
        return cls.cast(val, float)
    
    @classmethod
    def to_int(cls, key: str, default: float = 0):
        val = os.getenv(key, None)
        if val is None: return default
        return cls.cast(val, int)
    
    @classmethod
    def to_b64(cls, key: str, default: Any = ''):
        """ Base64 encoded """
        val = os.getenv(key, None)
        if val is None: return default
        from lazycls.serializers import Base
        return Base.b64_decode(val)

    @classmethod
    def to_b64_env(cls, src_key: str, to_key: str, default: Any = '', override: bool = False):
        """ used to handle credentials like AWS Keys, etc without directly exposing known env keys
            i.e. map src_key = AWS_KEYID -> to_key = AWS_ACCESS_KEY_ID
        """
        val = cls.to_b64(src_key, default)
        if val: cls.set_env(key=to_key, value=val, override=override)
        return val

    @classmethod
    def to_bgz(cls, key: str, default: Any = ''):
        """ Base64 + Gzip encoded """
        val = os.getenv(key, None)
        if val is None: return default
        from lazycls.serializers import Base
        return Base.b64_gzip_decode(val)
    
    @classmethod
    def to_bgz_env(cls, src_key: str, to_key: str, default: Any = '', override: bool = False):
        """ used to handle credentials like AWS Keys, etc without directly exposing known env keys
            i.e. map src_key = AWS_KEYID -> to_key = AWS_ACCESS_KEY_ID
        """
        val = cls.to_bgz(src_key, default)
        if val: cls.set_env(key=to_key, value=val, override=override)
        return val
    
    @classmethod
    def to_json(cls, src_key: str, to_key: str, to_path: Union[str, Path], default: str = '{}', override: bool = False, as_posix: bool = False):
        """ for things like GOOGLE_APPLICATION_CREDENTIALS that is json based
            we read the env src_key, attempt to load it and then dump it in to_path
            i.e. map src_key = ADC_DATA -> creates file at to_path -> sets env[to_key] = to_path (to_key = GOOGLE_APPLICATION_CREDENTIALS)
        """
        path = toPath(to_path, resolve=True)
        if not path.exists() or override: 
            data = os.getenv(src_key, default)
            from lazycls.serializers import OrJson
            path.write_text(OrJson.dumps(OrJson.loads(data)), encoding='utf-8')            
        cls.set_env(key=to_key, value=path.as_posix(), override=override)
        if as_posix: return path.as_posix()
        return path
    
    @classmethod
    def to_json_b64(cls, src_key: str, to_key: str, to_path: Union[str, Path], default: str = '{}', override: bool = False, as_posix: bool = False):
        """ for things like GOOGLE_APPLICATION_CREDENTIALS that is json based with base64 encoding
            we read the env src_key, attempt to load it and then dump it in to_path
            i.e. map src_key = ADC_DATA -> creates file at to_path -> sets env[to_key] = to_path (to_key = GOOGLE_APPLICATION_CREDENTIALS)
        """
        path = toPath(to_path, resolve=True)
        if not path.exists() or override: 
            from lazycls.serializers import OrJson, Base
            data = os.getenv(src_key, None)
            if data is not None: data = Base.b64_decode(data)
            else: data = default
            path.write_text(OrJson.dumps(OrJson.loads(data)), encoding='utf-8')
        cls.set_env(key=to_key, value=path.as_posix(), override=override)
        if as_posix: return path.as_posix()
        return path
    
    @classmethod
    def to_yaml(cls, src_key: str, to_key: str, to_path: Union[str, Path], default: str = '{}', override: bool = False, as_posix: bool = False):
        """ similar to to_json, just with yaml"""
        path = toPath(to_path, resolve=True)
        if not path.exists() or override: 
            data = os.getenv(src_key, default)
            from lazycls.serializers import Yaml
            path.write_text(Yaml.dumps(Yaml.loads(data)), encoding='utf-8')            
        cls.set_env(key=to_key, value=path.as_posix(), override=override)
        if as_posix: return path.as_posix()
        return path

    
    @classmethod
    def in_vals(cls, key: str, values: List[str] = [], exact: bool = False, **kwargs) -> bool:
        val = os.getenv(key, None)
        if val is None: return False
        return doesTextMatch(text=val, items=values, exact=exact, **kwargs)
    
    @classmethod
    def set_env(cls, key: str, value: Any, override: bool = False):
        if not cls.has(key) or override: os.environ[key] = str(value)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], override: bool = False):
        for key, val in data.items():
            cls.set_env(key=key, value=val, override=override)
    
    @classmethod
    def load_file(cls, path: Union[str, Path], override: bool = False):
        global _LoadedEnvFiles
        path = toPath(path, resolve=True)
        if not path.exists(): return None
        if path.name in _LoadedEnvFiles: return True
        loader = None
        if path.suffix == '.json': loader = OrJson.loads
        elif path.suffix == '.yaml': loader = Yaml.loads
        if loader is None: return False
        data = loader(path.read_text())
        for k,v in data.items():
            cls.set_env(name=k, value=v, override=override)
        _LoadedEnvFiles.add(path.name)
        return True

    @classmethod
    def encode_to_string(cls, value: Any):
        """ Transforms the value input into one that can be reloaded from env"""
        if not value: return 'none'
        if isinstance(value, str): return value
        if isinstance(value, type(Path)): return value.read_text(encoding='utf8')
        if isinstance(value, list): return cls.list_delimiter.join(value)
        if isinstance(value, int) or isinstance(value, float): return str(value)
        if isinstance(value, bool): return str(value).lower()
        if isinstance(value, dict):
            kvs = [f"{k}{cls.dict_delimiter}{v}" for k,v in value.items()]
            return cls.list_delimiter.join(kvs)
        return str(value)

    @classmethod
    def save_config(cls, configmap: Dict[str, Any], path: Union[str, Path]):
        path = toPath(path, resolve=True)
        cfg = {k: cls.encode_to_string(v) for k,v in configmap.items()}
        # now we encode it further
        from lazycls.serializers import Base, Yaml, OrJson
        cfg = {k: Base.b64_gzip_encode(v) for k,v in cfg.items()}
        path.write_text(Yaml.dumps(cfg) if path.suffix in {'.yml', '.yaml'} else OrJson.dumps(cfg))
        return path
    
    @classmethod
    def load_config(cls, path: Union[str, Path], set_as_env: bool = True, override: bool = False):
        # Should only expect our serialized format.
        path = toPath(path, resolve=True)
        assert path.exists(), f'Invalid Path: {path.as_posix()} does not exist.'
        from lazycls.serializers import Base, Yaml, OrJson        
        cfg = Yaml.loads(path.read_text()) if path.suffix in {'.yml', '.yaml'} else OrJson.loads(path.read_text())
        cfg = {k: Base.b64_gzip_decode(v) for k,v in cfg.items()}
        if set_as_env: 
            for k,v in cfg.items():
                cls.set_env(name=k, value=v, override=override)
        return cfg
        


__all__ = [
    'hasEnv',
    'getEnv',
    'envToBool',
    'envToType',
    'envToList',
    'envToDict',
    'envToFloat',
    'envToInt',
    'envInVals',
    'envToStr',
    'toEnv',
    'setEnvDict',
    'set_env_from_dict',
    'loadEnvFile',
    'load_env_file',
    'to_env',
    'Env'
]