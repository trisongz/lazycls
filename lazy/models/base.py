import orjson as _orjson
from pydantic import BaseModel, Field
from typing import List, Any
from .utils import to_camelcase


class OrJson:
    binary: False

    @classmethod
    def dumps(cls, obj, *args, default: Any = None, **kwargs):
        return _orjson.dumps(obj, default=default, *args, **kwargs).decode()

    @classmethod
    def loads(cls, obj, *args, **kwargs):
        return _orjson.loads(obj, *args, **kwargs)

class BaseCls(BaseModel):
    class Config:
        arbitrary_types_allowed = True
    
    def get(self, name, default: Any = None):
        return getattr(self, name, default)

class BaseLazy(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = 'allow'
        alias_generator = to_camelcase
        json_loads = OrJson.loads
        json_dumps = OrJson.dumps

    def get(self, name, default: Any = None):
        return getattr(self, name, default)

class Validator(BaseCls):
    text: str
    include: List[str] = []
    exclude: List[str] = []
    exact: bool = False

class ValidatorArgs(BaseCls):
    include: List[str] = []
    exclude: List[str] = []


class BaseDataCls(BaseCls):
    string: str = None
    value: Any = None
    dtype: str = None

__all__ = [
    'BaseModel',
    'Field',
    'BaseCls',
    'BaseDataCls',
    'BaseLazy'
]