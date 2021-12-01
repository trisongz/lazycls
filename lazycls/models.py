from pydantic import BaseModel, Field
from .types import *
from .serializers import OrJson
from .utils import to_camelcase


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

__all__ = [
    'BaseModel',
    'Field',
    'BaseCls'
    'BaseLazy'
]