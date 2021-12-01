import os
from pathlib import Path
from typing import Union
from .serializers import OrJson, Yaml
from .funcs import doesTextMatch
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


def toPath(path: Union[str, Path], resolve: bool = True) -> Path:
    if isinstance(path, str): path = Path(path)
    if resolve: path.resolve()
    return path


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
    'envToStr'
    'toEnv',
    'toPath',
    'loadEnvFile'
]