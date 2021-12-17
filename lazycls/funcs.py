import re
from datetime import datetime, timedelta
from functools import lru_cache, wraps
from .types import *
from .models import ValidatorArgs

def toList(text: Union[str, List[str]]):
    if isinstance(text, list): return text
    return [i for i in text.split(',') if i]


def setToMany(value: AnyMany) -> List[Any]:
    if not isinstance(value, list): value = [value]
    return value


def convertLog(*msgs, split_newline: bool = False):
    _msgs = []
    for msg in msgs:
        if isinstance(msg, list):
            for m in msg: _msgs.append(f'- {m}')
        elif isinstance(msg, dict):
            for k,v in msg.items():
                _msgs.append(f'- {k}: {v}')
        elif isinstance(msg, str):
            if split_newline:  _msgs.extend(msg.split('\n'))
            else: _msgs.append(msg)
        else: _msgs.append(f'{msg}')
    return [i for i in _msgs if i]

def doesTextValidate(text: str, include: List[str] = [], exclude: List[str] = [], exact: bool = False, **kwargs) -> bool:
    if not include and not exclude: return True
    _valid = False
    if exclude:
        for ex in exclude:
            if ((exact and ex == text)  or not exact and (ex in text or text in ex)): return False
        _valid = True
    if include:
        for inc in include:
            if ((exact and inc == text) or not exact and (inc in text or text in inc)): return True
    return _valid

def doesObjectListMatch(obj: List[Any], key: str = None, val: Any = None, exact: bool = False, **kwargs):
    if key and val and exact: return bool(len([i for i in obj if key in i and i.get(key) == val]) > 0)
    if key and exact: return bool(len([i for i in obj if key in i]) > 0)
    if val and exact: return bool(val in obj)
    if key and val: return bool(len([i for i in obj if key in i and (i.get(key) in val or val in i.get(key))]) > 0)
    if key: return bool(len([i for i in obj if (key in i or i in key)]) > 0)
    if val: return bool(val in obj or obj in val)
    return None

def doesObjectDictMatch(obj: DictAny, key: str = None, val: Any = None, exact: bool = False, allow_null: bool = False,  **kwargs):
    if key and val and exact: return bool(obj.get(key) == val)
    if key and exact and not allow_null: return bool(obj.get(key) is not None)
    if key and exact: return bool(str(obj.get(key, 'NULLVAL')) != 'NULLVAL')
    if val and exact: return bool(val in list(obj.values()))
    if key and val: return bool(obj.get(key) in val or val in obj.get(key))
    if key: return bool(obj.get(key))
    if val: return any(val in i or i in val for i in list(obj.values()))
    return None

def doesObjectStringMatch(obj: str, key: str = None, val: Any = None, exact: bool = False, **kwargs):
    if key and exact: return bool(obj == key)
    if val and exact: return bool(obj == val)
    if key: return bool(obj in key or key in obj)
    if val: return bool(obj in val or val in obj)
    return None


def doesObjectMatch(obj: Any, key: str = None, val: Any = None, exact: bool = False, allow_null: bool = False, **kwargs) -> bool:
    if not allow_null and obj is None: return False
    if isinstance(obj, list):
        rez = doesObjectListMatch(obj, key=key, val=val, exact=exact, **kwargs)
        if rez is not None: return rez
    if isinstance(obj, dict):
        rez = doesObjectDictMatch(obj, key=key, val=val, exact=exact, allow_null=allow_null, **kwargs)
        if rez is not None: return rez
    if isinstance(obj, str):
        rez = doesObjectStringMatch(obj, key=key, val=val, exact=exact, **kwargs)
        if rez is not None: return rez
    else:
        assert key is not None
        objval = getattr(obj, key, None)
        if val and exact and not allow_null: return bool(objval and objval == val)
        if not allow_null: return bool(objval is not None)
        if val: return bool(objval in val or val in objval)
    return False

def getObjectMatch(items: AnyMany, key: str, val: Any, returnMany: bool = False, exact: bool = False, allow_null: bool = False, **kwargs):
    items = setToMany(items)
    rez = []
    for i in items:
        if (doesObjectMatch(obj=i, key=key, val=val, exact=exact, allow_null=allow_null) and not returnMany): return i
        rez.append(i)
    return rez


def doesTextMatch(text: str, items: TextMany, exact: bool = False, valArgs: ValidatorArgs = None, **kwargs):
    items = setToMany(items)
    for i in items:
        if exact and i == text or (text in i or i in text):
            if valArgs: return doesTextValidate(i, exact=exact, **ValidatorArgs.dict())
            return True
    return False


def getTextMatch(text: str, items: TextMany, default: str = None, exact: bool = False, valArgs: ValidatorArgs = None, **kwargs):
    """Gets the First Matching Value from a List of String"""
    items = setToMany(items)
    for i in items:
        if exact and i == text or (text in i or i in text):
            if valArgs and doesTextValidate(i, exact=exact, **ValidatorArgs.dict()): return i
            return i
    return default

def getDictMatch(key: str, val: str, items: DictMany, default: DictAny = None, exact: bool = False, valArgs: ValidatorArgs = None, **kwargs):
    """Gets the First Matching Item from a Dict's Key = Value from a List of Dicts. Meant for Strings"""
    items = setToMany(items)
    for i in items:
        if exact and i.get(key, '') == val or (val in i.get(key, '') or i.get(key, '') in val):
            if valArgs and doesTextValidate(i[key], exact=exact, **ValidatorArgs.dict()): return i
            return i
    return default


def timed_cache(seconds: int, maxsize: int = 128):
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.utcnow() + func.lifetime
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.utcnow() >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.utcnow() + func.lifetime
            return func(*args, **kwargs)
        return wrapped_func
    return wrapper_cache


def cached_classmethod(seconds: int, maxsize: int = 128):
    def wrapper_cache(func):
        @classmethod
        @timed_cache(seconds = seconds, maxsize = maxsize)
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapped_func
    return wrapper_cache


def parse_metrics(data: Union[int, str]):
    if isinstance(data, int): return data
    if data.endswith('Ki'):
        return int(data.split('Ki')[0].strip()) * 1024
    if data.endswith('Mi'):
        return int(data.split('Mi')[0].strip()) * 1024**2
    if data.endswith('Gi'):
        return int(data.split('Gi')[0].strip()) * (1024**2 * 1024)
    if data.endswith('n'):
        return int(data.split('n')[0].strip())
    if data.endswith('m'):
        return int(data.split('m')[0].strip()) * 1000
    return int(data.strip())

def list_to_dict(data: List[Dict[str, str]], k: str, v: str):
    return {i[k]: i[v] for i in data}


def camelcase_to_snakecase(name):
    """
    Convert a name in camel case to snake case.
    Arguments:
    name -- The name to convert.
    Returns:
    The name in snake case.
    """
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


listToDict = list_to_dict
caseCamelToSnake = camelcase_to_snakecase
timedCache = timed_cache
parseMetrics = parse_metrics

__all__ = [
    'toList',
    'listToDict',
    'parseMetrics',
    'timedCache',
    'convertLog',
    'getObjectMatch',
    'doesTextMatch',
    'getTextMatch',
    'getDictMatch',
    'doesTextValidate',
    'setToMany',
    'caseCamelToSnake',
    'list_to_dict',
    'camelcase_to_snakecase',
    'timed_cache',
    'parse_metrics'
]